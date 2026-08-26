#!/usr/bin/env python3
"""Cascade Cycleworks plant-floor data producer.

Two sources in one process, mirroring what runs in production:

  --cdc         Stands in for the Openflow PostgreSQL CDC connector: it writes a
                CDC *journal* over Snowpipe Streaming, exactly as the connector
                does, and a scheduled MERGE task applies the journal to
                MFG.CDC.PRODUCTION_SCANS.

  --telemetry   Snowpipe Streaming into the Iceberg table
                MFG.RAW.STATION_TELEMETRY.

Only the *connector* is simulated. Everything downstream is real.

CDC modes
---------
  --cdc-mode journal   (default) faithful: journal table -> APPEND_ONLY stream
                       -> gated MERGE. Reproduces soft deletes, per-key dedup on
                       the LSN tuple, and the ~60s merge-gate latency.
  --cdc-mode direct    writes the settled result straight to PRODUCTION_SCANS
                       with ordinary DML. No journal, no stream, no task. Useful
                       as a fallback if the journal objects are missing.

Examples
--------
  # steady state, both sources
  python producer/producer.py --profile producer/profile.json --cdc --telemetry

  # the incident: humidity drifts, then PAINT defects spike ~90s later
  python producer/producer.py --profile producer/profile.json --cdc --telemetry --incident

  # the recovery: inspectors overturn failed frames, yield goes back up
  python producer/producer.py --profile producer/profile.json --cdc --telemetry --reinspect

  # see what it generates, no Snowflake account needed
  python producer/producer.py --dry-run --cdc
"""

import argparse
import copy
import json
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Plant model
# ---------------------------------------------------------------------------
LINES = ["WELD", "PAINT", "ASSEMBLY"]

STATION_BY_LINE = {"WELD": "ST-WELD-01", "PAINT": "ST-PAINT-01", "ASSEMBLY": "ST-ASSY-01"}

SKUS = ["FRAME-RD-54", "FRAME-RD-56", "FRAME-MTB-17", "FRAME-GRV-55"]

DEFECTS_BY_LINE = {
    "WELD": ["WELD_POROSITY", "WELD_MISALIGN"],
    "PAINT": ["PAINT_RUN", "PAINT_ORANGE_PEEL"],
    "ASSEMBLY": ["ASSY_TORQUE", "ASSY_MISSING_PART"],
}

# Healthy fraction of scans that FAIL, per line. Kept high enough that scrap is
# visible in a 5-minute bucket -- at 1% you cannot see the incident against the noise.
BASE_DEFECT_RATE = {"WELD": 0.040, "PAINT": 0.060, "ASSEMBLY": 0.035}

# During --incident, PAINT degrades to this. The cascade the agent must explain.
INCIDENT_DEFECT_RATE = {"PAINT": 0.260}

# The defect that dominates during the paint incident (humidity -> runs in the finish).
INCIDENT_DEFECT_CODE = "PAINT_RUN"

OPERATORS = [f"OP-{i:02d}" for i in range(1, 13)]

# Telemetry metrics: healthy centre and jitter.
METRICS = {
    "weld_current": ("WELD", 185.0, 4.0),
    "booth_humidity": ("PAINT", 44.0, 1.5),
    "booth_temp": ("PAINT", 22.5, 0.6),
    "torque_nm": ("ASSEMBLY", 12.0, 0.4),
}

# Humidity target once the drift kicks in. Cause precedes effect.
INCIDENT_HUMIDITY = 71.0

# ---------------------------------------------------------------------------
# Object names
# ---------------------------------------------------------------------------
SCANS_TABLE = "MFG.CDC.PRODUCTION_SCANS"

# The connector names its journal "<TABLE>_JOURNAL_<series>_<generation>", where
# series is epoch seconds at table registration and generation starts at 1 and
# increments on every schema change. We PIN the series so the lab has stable
# object names the skill can reference; in production it is not predictable.
JOURNAL_SERIES = "1787700000"
JOURNAL_GENERATION = "1"
JOURNAL_TABLE = f"PRODUCTION_SCANS_JOURNAL_{JOURNAL_SERIES}_{JOURNAL_GENERATION}"

TELEMETRY_DB, TELEMETRY_SCHEMA, TELEMETRY_TABLE = "MFG", "RAW", "STATION_TELEMETRY"
CDC_DB, CDC_SCHEMA = "MFG", "CDC"

# EVENT_TYPE literals. These are the connector's exact strings; the MERGE
# branches on them.
EV_INSERT = "IncrementalInsertRows"
EV_UPDATE = "IncrementalUpdateRows"
EV_DELETE = "IncrementalDeleteRows"

# Source columns, in order. Drives both the journal PAYLOAD__* columns and the
# destination table.
SOURCE_COLUMNS = ["SCAN_ID", "FRAME_ID", "LINE", "SKU", "STATUS", "DEFECT_CODE",
                  "STATION_ID", "OPERATOR_ID", "EVENT_TS", "UPDATED_TS"]

# The connector tags every merge it issues, which is how you find them in
# QUERY_HISTORY. Values are the connector's own.
MERGE_QUERY_TAG = json.dumps({
    "application": "SNOWFLAKE_OPENFLOW",
    "operation": "cdc.merge.full_values",
    "strategy": "full_values_snowflake_managed",
})

# The MERGE the connector's merge processor issues. Not a Snowflake task -- the
# connector runs this itself over its own Snowflake connection. See merge_loop().
MERGE_SQL = f"""
MERGE INTO MFG.CDC.PRODUCTION_SCANS AS TARGET
USING (
    SELECT * FROM (
        SELECT PRIMARY_KEY__SCAN_ID,
               PAYLOAD__SCAN_ID, PAYLOAD__FRAME_ID, PAYLOAD__LINE, PAYLOAD__SKU,
               PAYLOAD__STATUS, PAYLOAD__DEFECT_CODE, PAYLOAD__STATION_ID,
               PAYLOAD__OPERATOR_ID, PAYLOAD__EVENT_TS, PAYLOAD__UPDATED_TS,
               EVENT_TYPE,
               ROW_NUMBER() OVER (
                   PARTITION BY PRIMARY_KEY__SCAN_ID
                   ORDER BY MOST_SIGNIFICANT_POSITION DESC,
                            LEAST_SIGNIFICANT_POSITION DESC
               ) AS ROW_NUM
        FROM MFG.CDC.{JOURNAL_TABLE}_STREAM
        WHERE EVENT_TYPE IN ('{EV_INSERT}', '{EV_UPDATE}', '{EV_DELETE}')
    ) WHERE ROW_NUM = 1
) AS SOURCE
ON SOURCE.PRIMARY_KEY__SCAN_ID = TARGET.SCAN_ID
WHEN MATCHED AND SOURCE.EVENT_TYPE IN ('{EV_INSERT}', '{EV_UPDATE}') THEN
    UPDATE SET TARGET.SCAN_ID               = SOURCE.PAYLOAD__SCAN_ID,
               TARGET.FRAME_ID              = SOURCE.PAYLOAD__FRAME_ID,
               TARGET.LINE                  = SOURCE.PAYLOAD__LINE,
               TARGET.SKU                   = SOURCE.PAYLOAD__SKU,
               TARGET.STATUS                = SOURCE.PAYLOAD__STATUS,
               TARGET.DEFECT_CODE           = SOURCE.PAYLOAD__DEFECT_CODE,
               TARGET.STATION_ID            = SOURCE.PAYLOAD__STATION_ID,
               TARGET.OPERATOR_ID           = SOURCE.PAYLOAD__OPERATOR_ID,
               TARGET.EVENT_TS              = SOURCE.PAYLOAD__EVENT_TS,
               TARGET.UPDATED_TS            = SOURCE.PAYLOAD__UPDATED_TS,
               TARGET._SNOWFLAKE_DELETED    = FALSE,
               TARGET._SNOWFLAKE_UPDATED_AT = SYSDATE()
WHEN MATCHED AND SOURCE.EVENT_TYPE = '{EV_DELETE}' THEN
    UPDATE SET TARGET._SNOWFLAKE_DELETED    = TRUE,
               TARGET._SNOWFLAKE_UPDATED_AT = SYSDATE()
WHEN NOT MATCHED THEN
    INSERT (SCAN_ID, FRAME_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID,
            OPERATOR_ID, EVENT_TS, UPDATED_TS,
            _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
    VALUES (IFF(SOURCE.EVENT_TYPE = '{EV_DELETE}',
                SOURCE.PRIMARY_KEY__SCAN_ID, SOURCE.PAYLOAD__SCAN_ID),
            SOURCE.PAYLOAD__FRAME_ID, SOURCE.PAYLOAD__LINE, SOURCE.PAYLOAD__SKU,
            SOURCE.PAYLOAD__STATUS, SOURCE.PAYLOAD__DEFECT_CODE,
            SOURCE.PAYLOAD__STATION_ID, SOURCE.PAYLOAD__OPERATOR_ID,
            SOURCE.PAYLOAD__EVENT_TS, SOURCE.PAYLOAD__UPDATED_TS,
            SYSDATE(), SYSDATE(),
            IFF(SOURCE.EVENT_TYPE = '{EV_DELETE}', TRUE, FALSE))
"""

_stop = threading.Event()


def log(msg):
    """Progress goes to stderr so it does not flood a chat transcript when the
    producer runs in the background."""
    print(msg, file=sys.stderr, flush=True)


def utcnow():
    """Naive UTC. The account is UTC and Iceberg TIMESTAMP_NTZ wants no offset."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso(ts):
    return ts.isoformat(timespec="milliseconds") if isinstance(ts, datetime) else ts


# ---------------------------------------------------------------------------
# CDC source: stands in for the Openflow connector
# ---------------------------------------------------------------------------
class CdcSimulator:
    """Generates the change feed: inserts, re-inspection updates, and voids.

    Connector semantics reproduced here:
      * an UPDATE carries the OLD key in PRIMARY_KEY__* and the NEW values in
        PAYLOAD__*
      * a DELETE carries the key only -- every PAYLOAD__* is NULL
      * deletes are soft; the destination row is flagged, never removed
      * ordering is the (MOST_, LEAST_SIGNIFICANT_POSITION) tuple, monotonic
    """

    def __init__(self, args, sink):
        self.args = args
        self.sink = sink
        self.rng = random.Random(args.seed)
        self.frame_seq = 0
        self.rows = {}            # scan_id -> current row state
        self.recent_fails = []    # scan_ids eligible for re-inspection
        self.recent_scans = []    # scan_ids eligible for voiding
        self.incident_until = None
        self.counts = {"insert": 0, "update": 0, "delete": 0}
        # Logical WAL clock. batch = transaction, msg = position within it.
        self.batch = 0

    # -- incident control ---------------------------------------------------
    def defect_rate(self, line):
        if self.incident_active() and line in INCIDENT_DEFECT_RATE:
            return INCIDENT_DEFECT_RATE[line]
        return BASE_DEFECT_RATE[line]

    def incident_active(self):
        return self.incident_until is not None and utcnow() < self.incident_until

    def start_incident(self, minutes):
        self.incident_until = utcnow() + timedelta(minutes=minutes)
        log(f"[cdc] PAINT defect rate -> {INCIDENT_DEFECT_RATE['PAINT']:.0%} "
            f"for {minutes} min")

    # -- generation ---------------------------------------------------------
    def new_scan(self):
        self.frame_seq += 1
        line = self.rng.choices(LINES, weights=[34, 33, 33])[0]
        failed = self.rng.random() < self.defect_rate(line)

        if failed:
            if self.incident_active() and line == "PAINT":
                # Skew hard to one code so "which defect is driving scrap" has an answer.
                defect = INCIDENT_DEFECT_CODE if self.rng.random() < 0.8 \
                    else self.rng.choice(DEFECTS_BY_LINE[line])
            else:
                defect = self.rng.choice(DEFECTS_BY_LINE[line])
        else:
            defect = None

        now = utcnow()
        return {
            "SCAN_ID": f"S-{uuid.uuid4().hex[:12]}",
            "FRAME_ID": f"F-{self.frame_seq:06d}",
            "LINE": line,
            "SKU": self.rng.choice(SKUS),
            "STATUS": "FAIL" if failed else "PASS",
            "DEFECT_CODE": defect,
            "STATION_ID": STATION_BY_LINE[line],
            "OPERATOR_ID": self.rng.choice(OPERATORS),
            "EVENT_TS": now,
            "UPDATED_TS": now,
        }

    def tick(self, n):
        """One transaction: n inserts, plus any updates and deletes now due."""
        self.batch += 1
        msn = self.batch * 10_000     # end-of-transaction LSN
        msg = 0

        def next_lsn():
            nonlocal msg
            msg += 1
            return msn + msg

        rows = [self.new_scan() for _ in range(n)]
        fails_this_tick = 0
        for r in rows:
            self.rows[r["SCAN_ID"]] = r
            self.sink.emit_insert(r, msn, next_lsn())
            self.counts["insert"] += 1
            self.recent_scans.append(r["SCAN_ID"])
            if r["STATUS"] == "FAIL":
                self.recent_fails.append(r["SCAN_ID"])
                fails_this_tick += 1
        self.recent_scans = self.recent_scans[-4000:]
        self.recent_fails = self.recent_fails[-2000:]

        # UPDATE: an inspector re-checks a failed frame and passes it. This is the
        # case that forces aggregates to DECREASE, which append-only pipelines get
        # wrong.
        #
        # Rate is a fraction of FAILS, not of all scans -- you cannot overturn more
        # frames than actually failed. Expressing it against inserts (as an earlier
        # draft did) silently overturned every failure and pinned yield at 100%.
        if self.args.reinspect:
            # Burst mode: drain the backlog so yield visibly recovers.
            n_upd = max(1, int(len(self.recent_fails) * 0.10))
        else:
            n_upd = self._poisson_ish(fails_this_tick * self.args.update_rate)

        for _ in range(n_upd):
            if not self.recent_fails:
                break
            scan_id = self.recent_fails.pop(self.rng.randrange(len(self.recent_fails)))
            row = self.rows.get(scan_id)
            if row is None:
                continue
            updated = copy.copy(row)
            updated["STATUS"] = "PASS"
            updated["DEFECT_CODE"] = None
            updated["UPDATED_TS"] = utcnow()
            self.rows[scan_id] = updated
            # old key, new payload -- the connector's UPDATE shape
            self.sink.emit_update(scan_id, updated, msn, next_lsn())
            self.counts["update"] += 1

        # DELETE: a duplicate barcode scan is voided. Soft delete downstream.
        n_del = self._poisson_ish(len(rows) * self.args.delete_rate)
        for _ in range(n_del):
            if not self.recent_scans:
                break
            scan_id = self.recent_scans.pop(self.rng.randrange(len(self.recent_scans)))
            if scan_id not in self.rows:
                continue
            self.sink.emit_delete(scan_id, msn, next_lsn())
            self.rows.pop(scan_id, None)
            self.counts["delete"] += 1

        self.sink.tick_done()

    def _poisson_ish(self, expected):
        base = int(expected)
        return base + (1 if self.rng.random() < (expected - base) else 0)


# ---------------------------------------------------------------------------
# Telemetry source
# ---------------------------------------------------------------------------
class TelemetrySimulator:
    def __init__(self, args, sink):
        self.args = args
        self.sink = sink
        self.rng = random.Random((args.seed or 0) + 7)
        self.drift_start = None
        self.ramp = 1.0

    def start_drift(self, ramp_seconds):
        self.drift_start = utcnow()
        self.ramp = max(1.0, ramp_seconds)
        log(f"[telem] booth_humidity ramping {METRICS['booth_humidity'][1]:.0f} -> "
            f"{INCIDENT_HUMIDITY:.0f} over {ramp_seconds:.0f}s")

    def current_humidity(self):
        base = METRICS["booth_humidity"][1]
        if self.drift_start is None:
            return base
        frac = min(1.0, (utcnow() - self.drift_start).total_seconds() / self.ramp)
        return base + (INCIDENT_HUMIDITY - base) * frac

    def batch(self, n):
        rows = []
        for _ in range(n):
            metric = self.rng.choice(list(METRICS))
            line, centre, jitter = METRICS[metric]
            if metric == "booth_humidity":
                centre = self.current_humidity()
            rows.append({
                "STATION_ID": STATION_BY_LINE[line],
                "LINE": line,
                "METRIC": metric,
                "VALUE": round(self.rng.gauss(centre, jitter), 3),
                "EVENT_TS": iso(utcnow()),
            })
        return rows


# ---------------------------------------------------------------------------
# CDC sinks
# ---------------------------------------------------------------------------
class CdcSink:
    def emit_insert(self, row, msn, lsn):
        raise NotImplementedError

    def emit_update(self, old_key, new_row, msn, lsn):
        raise NotImplementedError

    def emit_delete(self, key, msn, lsn):
        raise NotImplementedError

    def tick_done(self):
        pass

    def close(self):
        pass


def journal_event(pk, event_type, row, msn, lsn):
    """The connector's flat 'Snowflake Journal' wire shape.

    PRIMARY_KEY__* is the OLD key (identical to the new one here -- this lab's
    replication key is immutable). On DELETE every PAYLOAD__* is NULL.
    """
    ev = {
        "PRIMARY_KEY__SCAN_ID": pk,
        "LEAST_SIGNIFICANT_POSITION": lsn,
        "MOST_SIGNIFICANT_POSITION": msn,
        "EVENT_TYPE": event_type,
        "SEEN_AT": iso(utcnow()),
        # A JSON *string*, not a native object -- this is what the connector
        # writes, so reading it back needs PARSE_JSON(SF_METADATA::STRING).
        "SF_METADATA": json.dumps({"offset_token": str(lsn)}),
    }
    for c in SOURCE_COLUMNS:
        ev[f"PAYLOAD__{c}"] = None if row is None else iso(row.get(c))
    return ev


class JournalSink(CdcSink):
    """Faithful path: Snowpipe Streaming into the CDC journal table, plus the
    merge processor that applies it.

    Both halves live here because both live in the connector. The connector does
    NOT create a Snowflake task -- its merge processor is timer-driven inside the
    connector runtime and issues the MERGE itself over its own Snowflake
    connection, gated by a CRON expression that decides *when* a batch becomes
    eligible. That is what merge_loop() reproduces.
    """

    def __init__(self, profile_path, profile, merge=True):
        from snowflake.ingest.streaming import StreamingIngestClient

        self.client = StreamingIngestClient(
            client_name="cascade_cdc_journal",
            db_name=CDC_DB,
            schema_name=CDC_SCHEMA,
            pipe_name=f"{JOURNAL_TABLE}-STREAMING",
            profile_json=profile_path,
        )
        self.channel, _ = self.client.open_channel(channel_name="cdc_journal_1")

        # Separate SQL connection for the merge, exactly as the connector has.
        self.cn = None
        self.merges = 0
        self.rows_merged = 0
        if merge:
            import snowflake.connector as sc

            self.cn = sc.connect(
                account=profile["account"], user=profile["user"],
                password=profile["personal_access_token"],
                role="ACCOUNTADMIN", warehouse="HOL_WH",
                database=CDC_DB, schema=CDC_SCHEMA,
                client_session_keep_alive=True,
                session_parameters={"QUERY_TAG": MERGE_QUERY_TAG},
            )
            self.lock = threading.Lock()

    def _send(self, ev, lsn):
        self.channel.append_row(ev, offset_token=str(lsn))

    def emit_insert(self, row, msn, lsn):
        self._send(journal_event(row["SCAN_ID"], EV_INSERT, row, msn, lsn), lsn)

    def emit_update(self, old_key, new_row, msn, lsn):
        self._send(journal_event(old_key, EV_UPDATE, new_row, msn, lsn), lsn)

    def emit_delete(self, key, msn, lsn):
        self._send(journal_event(key, EV_DELETE, None, msn, lsn), lsn)

    def run_merge(self):
        """Apply everything currently queued in the journal's stream.

        Returns (rows_affected, seconds). Reading the STREAM inside a committed
        statement is what advances the offset, so this is exactly-once.
        """
        if self.cn is None:
            return (0, 0.0)
        started = time.time()
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.execute(MERGE_SQL)
                affected = cur.rowcount or 0
            finally:
                cur.close()
        self.merges += 1
        self.rows_merged += affected
        return (affected, time.time() - started)

    def close(self):
        try:
            self.channel.close()
            self.client.close()
        except Exception:
            pass
        try:
            if self.cn is not None:
                self.cn.close()
        except Exception:
            pass


INSERT_SQL = f"""
INSERT INTO {SCANS_TABLE}
  (SCAN_ID, FRAME_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID, OPERATOR_ID,
   EVENT_TS, UPDATED_TS, _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

UPDATE_SQL = f"""
UPDATE {SCANS_TABLE}
   SET STATUS = %s, DEFECT_CODE = %s, UPDATED_TS = %s, _SNOWFLAKE_UPDATED_AT = %s
 WHERE SCAN_ID = %s AND _SNOWFLAKE_DELETED = FALSE
"""

VOID_SQL = f"""
UPDATE {SCANS_TABLE}
   SET _SNOWFLAKE_DELETED = TRUE, _SNOWFLAKE_UPDATED_AT = %s
 WHERE SCAN_ID = %s
"""


class DirectDmlSink(CdcSink):
    """Fallback path: writes the settled result the MERGE would have produced.

    Skips the journal, the stream and the task entirely, so it loses the
    observable ~60s merge gate and the two-path design -- but it needs no CDC
    objects and lands rows in a second.
    """

    def __init__(self, profile):
        import snowflake.connector as sc

        self.cn = sc.connect(
            account=profile["account"], user=profile["user"],
            password=profile["personal_access_token"],
            role="ACCOUNTADMIN", warehouse="HOL_WH",
            database=CDC_DB, schema=CDC_SCHEMA,
            client_session_keep_alive=True,
        )
        self.lock = threading.Lock()
        self.pending = []

    def emit_insert(self, row, msn, lsn):
        now = utcnow()
        self.pending.append((
            row["SCAN_ID"], row["FRAME_ID"], row["LINE"], row["SKU"], row["STATUS"],
            row["DEFECT_CODE"], row["STATION_ID"], row["OPERATOR_ID"],
            row["EVENT_TS"], row["UPDATED_TS"], now, now, False))

    def emit_update(self, old_key, new_row, msn, lsn):
        self._exec(UPDATE_SQL, (new_row["STATUS"], new_row["DEFECT_CODE"],
                                new_row["UPDATED_TS"], utcnow(), old_key))

    def emit_delete(self, key, msn, lsn):
        self._exec(VOID_SQL, (utcnow(), key))

    def tick_done(self):
        if not self.pending:
            return
        batch, self.pending = self.pending, []
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.executemany(INSERT_SQL, batch)
            finally:
                cur.close()

    def _exec(self, sql, params):
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.execute(sql, params)
            finally:
                cur.close()

    def close(self):
        try:
            self.tick_done()
            self.cn.close()
        except Exception:
            pass


class DryRunCdcSink(CdcSink):
    def __init__(self, mode):
        self.mode = mode

    def _out(self, obj):
        sys.stdout.write(json.dumps(obj, default=str) + "\n")

    def emit_insert(self, row, msn, lsn):
        self._out(journal_event(row["SCAN_ID"], EV_INSERT, row, msn, lsn)
                  if self.mode == "journal" else {"op": "INSERT", **row})

    def emit_update(self, old_key, new_row, msn, lsn):
        self._out(journal_event(old_key, EV_UPDATE, new_row, msn, lsn)
                  if self.mode == "journal" else {"op": "UPDATE", **new_row})

    def emit_delete(self, key, msn, lsn):
        self._out(journal_event(key, EV_DELETE, None, msn, lsn)
                  if self.mode == "journal" else {"op": "DELETE(soft)", "SCAN_ID": key})


# ---------------------------------------------------------------------------
# Telemetry sinks
# ---------------------------------------------------------------------------
class TelemetrySink:
    def __init__(self, profile_path):
        from snowflake.ingest.streaming import StreamingIngestClient

        self.client = StreamingIngestClient(
            client_name="cascade_telemetry_producer",
            db_name=TELEMETRY_DB,
            schema_name=TELEMETRY_SCHEMA,
            pipe_name=f"{TELEMETRY_TABLE}-STREAMING",
            profile_json=profile_path,
        )
        self.channel, _ = self.client.open_channel(channel_name="telemetry_1")
        self.offset = 0

    def send(self, rows):
        for r in rows:
            self.channel.append_row(r, offset_token=str(self.offset))
            self.offset += 1

    def close(self):
        try:
            self.channel.close()
            self.client.close()
        except Exception:
            pass


class DryRunTelemetrySink:
    def send(self, rows):
        for r in rows:
            sys.stdout.write("TELEM " + json.dumps(r) + "\n")

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------
def merge_loop(sink, gate_seconds):
    """The connector's merge control loop.

    The processor itself is timer-driven and continuous; a CRON expression acts
    as an *eligibility gate* deciding when queued changes become mergeable. The
    flow default is `0 * * * * ?` -- second :00 of every minute -- so changes
    queue for up to a minute before a merge picks them up. That gate, not
    throughput, is where the CDC latency in this lab comes from: the merge itself
    takes a second or two.

    Align to the wall clock so the gate lands on :00 and the lag is easy to
    stopwatch.
    """
    while not _stop.is_set():
        now = time.time()
        wait = gate_seconds - (now % gate_seconds)
        if _stop.wait(wait):
            break
        try:
            affected, secs = sink.run_merge()
        except Exception as exc:
            log(f"[merge] error: {exc}")
            continue
        if affected:
            log(f"[merge] gate fired: {affected} rows applied in {secs:.1f}s "
                f"(merges={sink.merges} rows_total={sink.rows_merged})")
        else:
            # Nothing queued. The connector yields here rather than merging.
            log("[merge] gate fired: nothing queued, skipped")


def cdc_loop(sim, rate, status_every=15.0):
    last_status = time.time()
    while not _stop.is_set():
        started = time.time()
        try:
            sim.tick(max(1, int(round(rate))))
        except Exception as exc:
            log(f"[cdc] error: {exc}")
            time.sleep(2.0)
            continue
        if time.time() - last_status >= status_every:
            c = sim.counts
            log(f"[cdc] inserts={c['insert']} updates={c['update']} "
                f"soft_deletes={c['delete']}"
                f"{'  INCIDENT' if sim.incident_active() else ''}")
            last_status = time.time()
        _stop.wait(max(0.0, 1.0 - (time.time() - started)))


def telemetry_loop(sim, sink, rate, status_every=15.0):
    sent = 0
    last_status = time.time()
    while not _stop.is_set():
        started = time.time()
        try:
            rows = sim.batch(max(1, int(round(rate))))
            sink.send(rows)
            sent += len(rows)
        except Exception as exc:
            log(f"[telem] error: {exc}")
            time.sleep(2.0)
            continue
        if time.time() - last_status >= status_every:
            log(f"[telem] rows={sent} booth_humidity~{sim.current_humidity():.1f}")
            last_status = time.time()
        _stop.wait(max(0.0, 1.0 - (time.time() - started)))


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Cascade Cycleworks plant-floor producer (CDC + telemetry).")
    ap.add_argument("--profile", help="path to profile.json (required unless --dry-run)")
    ap.add_argument("--cdc", action="store_true", help="run the CDC source")
    ap.add_argument("--telemetry", action="store_true", help="run the telemetry source")
    ap.add_argument("--cdc-mode", choices=["journal", "direct"], default="journal",
                    help="journal = faithful connector path (default); "
                         "direct = write the settled result straight to the table")
    ap.add_argument("--merge-gate-seconds", type=float, default=60.0,
                    help="the connector's CRON merge-eligibility gate, in seconds "
                         "(default 60, matching the flow default of second :00 every "
                         "minute). Lower it to watch CDC latency fall.")
    ap.add_argument("--no-merge", action="store_true",
                    help="write the journal but never merge it, so you can watch the "
                         "journal fill while the destination table stays empty")
    ap.add_argument("--rate", type=float, default=2.0, help="scans/sec (default 2)")
    ap.add_argument("--telemetry-rate", type=float, default=60.0,
                    help="telemetry rows/sec (default 60)")
    ap.add_argument("--update-rate", type=float, default=0.15,
                    help="fraction of FAILED frames later overturned to PASS (default 0.15)")
    ap.add_argument("--delete-rate", type=float, default=0.005,
                    help="voided scans as a fraction of inserts (default 0.005)")
    ap.add_argument("--incident", action="store_true",
                    help="humidity drift, then a PAINT defect spike ~90s later")
    ap.add_argument("--incident-after", type=float, default=90.0,
                    help="seconds of humidity drift before defects spike (default 90)")
    ap.add_argument("--incident-minutes", type=float, default=20.0,
                    help="how long the defect spike lasts (default 20)")
    ap.add_argument("--reinspect", action="store_true",
                    help="burst of re-inspections: watch yield RECOVER")
    ap.add_argument("--duration", type=float, default=0.0,
                    help="stop after N seconds (default: run until Ctrl-C)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="print to stdout, no Snowflake connection")
    args = ap.parse_args()

    if not args.cdc and not args.telemetry:
        args.cdc = args.telemetry = True
    if not args.dry_run and not args.profile:
        ap.error("--profile is required unless --dry-run")

    profile = None
    if not args.dry_run:
        with open(args.profile) as fh:
            profile = json.load(fh)

    def handle_stop(_sig, _frm):
        log("stopping...")
        _stop.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    threads, sinks = [], []
    cdc_sim = telem_sim = None

    if args.cdc:
        if args.dry_run:
            sink = DryRunCdcSink(args.cdc_mode)
        elif args.cdc_mode == "journal":
            sink = JournalSink(args.profile, profile, merge=not args.no_merge)
        else:
            sink = DirectDmlSink(profile)
        sinks.append(sink)
        cdc_sim = CdcSimulator(args, sink)
        threads.append(threading.Thread(target=cdc_loop, args=(cdc_sim, args.rate),
                                        daemon=True, name="cdc"))
        # The merge processor is part of the connector, so it runs here -- not as
        # a Snowflake task. See merge_loop().
        if args.cdc_mode == "journal" and not args.dry_run and not args.no_merge:
            threads.append(threading.Thread(
                target=merge_loop, args=(sink, args.merge_gate_seconds),
                daemon=True, name="merge"))

    if args.telemetry:
        sink = DryRunTelemetrySink() if args.dry_run else TelemetrySink(args.profile)
        sinks.append(sink)
        telem_sim = TelemetrySimulator(args, sink)
        threads.append(threading.Thread(target=telemetry_loop,
                                        args=(telem_sim, sink, args.telemetry_rate),
                                        daemon=True, name="telem"))

    log(f"starting: cdc={args.cdc} ({args.cdc_mode}) telemetry={args.telemetry} "
        f"scans/s={args.rate} telem/s={args.telemetry_rate}")
    if args.cdc and args.cdc_mode == "journal" and not args.dry_run:
        log(f"[cdc] journal: {CDC_DB}.{CDC_SCHEMA}.{JOURNAL_TABLE}")
        if args.no_merge:
            log("[merge] DISABLED (--no-merge): the destination table will not change")
        else:
            log(f"[merge] this process issues the MERGE itself, on a "
                f"{args.merge_gate_seconds:.0f}s CRON gate -- the connector does not "
                f"create a Snowflake task")

    for t in threads:
        t.start()

    # The cascade: cause (humidity) leads effect (defects) by --incident-after.
    if args.incident:
        if telem_sim:
            telem_sim.start_drift(args.incident_after)
        if cdc_sim:
            def arm():
                if not _stop.wait(args.incident_after):
                    cdc_sim.start_incident(args.incident_minutes)
            threading.Thread(target=arm, daemon=True).start()

    try:
        if args.duration:
            _stop.wait(args.duration)
        else:
            while not _stop.is_set():
                _stop.wait(1.0)
    finally:
        _stop.set()
        for t in threads:
            t.join(timeout=15)
        for s in sinks:
            s.close()
        if cdc_sim:
            c = cdc_sim.counts
            log(f"final cdc: inserts={c['insert']} updates={c['update']} "
                  f"soft_deletes={c['delete']}", flush=True)
        log("stopped")


if __name__ == "__main__":
    main()
