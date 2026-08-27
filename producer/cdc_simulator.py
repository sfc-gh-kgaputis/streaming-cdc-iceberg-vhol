"""The simulated Openflow Postgres CDC connector.

Read this file if you want to know what the connector actually does. It is the
only part of the pipeline that is simulated, and it is faithful in the ways that
matter: it creates its own target objects, appends change events to a journal over
Snowpipe Streaming, and applies that journal with a MERGE it issues itself on a
CRON eligibility gate. There is no Snowflake TASK anywhere, because the real
connector does not create one.

Where to look, if you are reading rather than running:

    MERGE_SQL           the statement the connector applies the journal with
    *_DDL + ensure_objects()   the three objects it creates for itself
    journal_event()     the connector's wire format -- PRIMARY_KEY__ vs PAYLOAD__
    JournalSink         the faithful path: streaming append + the merge processor
    merge_loop()        the CRON gate, and where the CDC latency really comes from

Those five implement the connector path. `CdcSimulator` generates the plant's
events, `DryRunCdcSink` serves `--dry-run` in Setup D, and `DirectDmlSink` backs
`--cdc-mode direct`, which writes to the destination table and skips the journal.
"""

from __future__ import annotations

import copy
import json
import random
import sys
import threading
import time
from datetime import timedelta
from typing import Any

from common import (
    JOURNAL_DB,
    JOURNAL_SCHEMA,
    JOURNAL_TABLE,
    LINES,
    REINSPECT_FRACTION,
    REINSPECT_MINUTES,
    SCANS_TABLE,
    STATION_BY_LINE,
    _stop,
    connect_sql,
    iso,
    log,
    utcnow,
)

SKUS = ["FRAME-RD-54", "FRAME-RD-56", "FRAME-MTB-17", "FRAME-GRV-55"]

DEFECTS_BY_LINE = {
    "WELD": ["WELD_POROSITY", "WELD_MISALIGN"],
    "PAINT": ["PAINT_RUN", "PAINT_ORANGE_PEEL"],
    "ASSEMBLY": ["ASSY_TORQUE", "ASSY_MISSING_PART"],
}

# Healthy fraction of scans that FAIL, per line. Kept high enough that scrap is
# visible in a 5-minute bucket -- at 1% you cannot see the incident against the noise.
BASE_DEFECT_RATE = {"WELD": 0.040, "PAINT": 0.040, "ASSEMBLY": 0.035}

# During --incident, PAINT degrades to this. The cascade the agent must explain.
INCIDENT_DEFECT_RATE = {"PAINT": 0.260}

# The defect that dominates during the paint incident (humidity -> runs in the finish).
INCIDENT_DEFECT_CODE = "PAINT_RUN"

OPERATORS = [f"OP-{i:02d}" for i in range(1, 13)]


EV_INSERT = "IncrementalInsertRows"
EV_UPDATE = "IncrementalUpdateRows"
EV_DELETE = "IncrementalDeleteRows"

# Source columns, in order. Drives both the journal PAYLOAD__* columns and the
# destination table.
SOURCE_COLUMNS = [
    "INSPECTION_ID",
    "UNIT_ID",
    "LINE",
    "SKU",
    "STATUS",
    "DEFECT_CODE",
    "STATION_ID",
    "OPERATOR_ID",
    "EVENT_TS",
    "UPDATED_TS",
]

# The connector tags every merge it issues, which is how you find them in
# QUERY_HISTORY. Values are the connector's own.
MERGE_QUERY_TAG = json.dumps(
    {
        "application": "SNOWFLAKE_OPENFLOW",
        "operation": "cdc.merge.full_values",
        "strategy": "full_values_snowflake_managed",
    }
)

# The MERGE the connector's merge processor issues. Not a Snowflake task -- the
# connector runs this itself over its own Snowflake connection. See merge_loop().
MERGE_SQL = f"""
MERGE INTO MFG.RAW.QUALITY_INSPECTIONS AS TARGET
USING (
    SELECT * FROM (
        SELECT PRIMARY_KEY__INSPECTION_ID,
               PAYLOAD__INSPECTION_ID, PAYLOAD__UNIT_ID, PAYLOAD__LINE, PAYLOAD__SKU,
               PAYLOAD__STATUS, PAYLOAD__DEFECT_CODE, PAYLOAD__STATION_ID,
               PAYLOAD__OPERATOR_ID, PAYLOAD__EVENT_TS, PAYLOAD__UPDATED_TS,
               EVENT_TYPE,
               ROW_NUMBER() OVER (
                   PARTITION BY PRIMARY_KEY__INSPECTION_ID
                   ORDER BY MOST_SIGNIFICANT_POSITION DESC,
                            LEAST_SIGNIFICANT_POSITION DESC
               ) AS ROW_NUM
        FROM MFG.RAW.{JOURNAL_TABLE}_STREAM
        WHERE EVENT_TYPE IN ('{EV_INSERT}', '{EV_UPDATE}', '{EV_DELETE}')
    ) WHERE ROW_NUM = 1
) AS SOURCE
ON SOURCE.PRIMARY_KEY__INSPECTION_ID = TARGET.INSPECTION_ID
WHEN MATCHED AND SOURCE.EVENT_TYPE IN ('{EV_INSERT}', '{EV_UPDATE}') THEN
    UPDATE SET TARGET.INSPECTION_ID          = SOURCE.PAYLOAD__INSPECTION_ID,
               TARGET.UNIT_ID               = SOURCE.PAYLOAD__UNIT_ID,
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
    INSERT (INSPECTION_ID, UNIT_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID,
            OPERATOR_ID, EVENT_TS, UPDATED_TS,
            _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
    VALUES (IFF(SOURCE.EVENT_TYPE = '{EV_DELETE}',
                SOURCE.PRIMARY_KEY__INSPECTION_ID, SOURCE.PAYLOAD__INSPECTION_ID),
            SOURCE.PAYLOAD__UNIT_ID, SOURCE.PAYLOAD__LINE, SOURCE.PAYLOAD__SKU,
            SOURCE.PAYLOAD__STATUS, SOURCE.PAYLOAD__DEFECT_CODE,
            SOURCE.PAYLOAD__STATION_ID, SOURCE.PAYLOAD__OPERATOR_ID,
            SOURCE.PAYLOAD__EVENT_TS, SOURCE.PAYLOAD__UPDATED_TS,
            SYSDATE(), SYSDATE(),
            IFF(SOURCE.EVENT_TYPE = '{EV_DELETE}', TRUE, FALSE))
"""


# ---------------------------------------------------------------------------
# The objects the connector creates for itself
# ---------------------------------------------------------------------------
# A real Openflow connector creates its own targets. From the PostgreSQL connector
# docs: on running the flow it "creates a schema for journal tables" and "creates
# the schemas and destination tables matching the source tables configured for
# replication." Nobody hand-builds a CDC destination table in production -- you
# point the connector at a source and the objects appear. So they appear here too.
#
# What this deliberately does NOT create: the database, the schemas, or their
# Iceberg defaults. Those are the attendee's job in Part 1, because that is where
# the ICEBERG_VERSION_DEFAULT session-schema lesson lives.
#
# Every storage property below is stated EXPLICITLY rather than inherited. That is
# on purpose: this file is not the teaching surface for inheritance, so it should be
# immune to the trap instead of demonstrating it. The one table that inherits --
# and proves inheritance works -- is STATION_TELEMETRY, which the attendee creates.

# Standard, not Iceberg, and deliberately so: this table takes UPDATEs and DELETEs
# continuously, which is the entire point of a change feed.
DESTINATION_DDL = f"""
CREATE TABLE IF NOT EXISTS {SCANS_TABLE} (
  INSPECTION_ID           STRING,          -- replication key (the Postgres PK)
  UNIT_ID                 STRING,
  LINE                    STRING,          -- WELD | PAINT | ASSEMBLY
  SKU                     STRING,
  STATUS                  STRING,          -- PASS | FAIL
  DEFECT_CODE             STRING,          -- NULL on PASS
  STATION_ID              STRING,          -- joins to telemetry
  OPERATOR_ID             STRING,
  EVENT_TS                TIMESTAMP_NTZ,
  UPDATED_TS              TIMESTAMP_NTZ,   -- source-system modification time
  _SNOWFLAKE_INSERTED_AT  TIMESTAMP_NTZ,   -- connector-maintained
  _SNOWFLAKE_UPDATED_AT   TIMESTAMP_NTZ,   -- connector-maintained
  _SNOWFLAKE_DELETED      BOOLEAN          -- connector-maintained SOFT delete
)
"""

JOURNAL_DDL = f"""
CREATE ICEBERG TABLE IF NOT EXISTS {JOURNAL_DB}.{JOURNAL_SCHEMA}.{JOURNAL_TABLE} (
  PRIMARY_KEY__INSPECTION_ID  STRING        NOT NULL,
  PAYLOAD__INSPECTION_ID      STRING,
  PAYLOAD__UNIT_ID            STRING,
  PAYLOAD__LINE               STRING,
  PAYLOAD__SKU                STRING,
  PAYLOAD__STATUS             STRING,
  PAYLOAD__DEFECT_CODE        STRING,
  PAYLOAD__STATION_ID         STRING,
  PAYLOAD__OPERATOR_ID        STRING,
  PAYLOAD__EVENT_TS           TIMESTAMP_NTZ,
  PAYLOAD__UPDATED_TS         TIMESTAMP_NTZ,
  LEAST_SIGNIFICANT_POSITION  NUMBER(38,0),   -- bare NUMBER is rejected by Iceberg
  MOST_SIGNIFICANT_POSITION   NUMBER(38,0),
  EVENT_TYPE                  STRING        NOT NULL,
  SEEN_AT                     TIMESTAMP_NTZ,
  SF_METADATA                 VARIANT         -- VARIANT is why this needs v3
)
  EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
  CATALOG = 'SNOWFLAKE'
  ICEBERG_VERSION = 3
  ERROR_LOGGING = TRUE
"""

# APPEND_ONLY because a journal only ever gets appends. Reading the STREAM rather
# than the table is what gives exactly-once: the offset advances only when the
# consuming DML commits.
STREAM_DDL = f"""
CREATE STREAM IF NOT EXISTS {JOURNAL_DB}.{JOURNAL_SCHEMA}.{JOURNAL_TABLE}_STREAM
  ON TABLE {JOURNAL_DB}.{JOURNAL_SCHEMA}.{JOURNAL_TABLE}
  APPEND_ONLY = TRUE
"""


def ensure_objects(profile: dict[str, Any]) -> None:
    """Create the destination table, the journal and its stream, as the connector does.

    IF NOT EXISTS throughout, so a re-run is safe and an attendee who already built
    these by hand is not clobbered.

    Fails loudly rather than limping: if the schema or its Iceberg defaults are
    missing the attendee has not finished Part 1, and every later step would fail in
    a more confusing place.
    """
    cn = connect_sql(profile)
    try:
        cur = cn.cursor()
        # USE SCHEMA first. Belt and braces given the explicit clauses above, but it
        # keeps this file modelling the discipline the lab teaches.
        cur.execute(f"USE SCHEMA {JOURNAL_DB}.{JOURNAL_SCHEMA}")
        for label, sql in (
            ("destination table", DESTINATION_DDL),
            ("journal", JOURNAL_DDL),
            ("journal stream", STREAM_DDL),
        ):
            cur.execute(sql)
            log(f"[connector] {label} ready")
        cur.close()
    except Exception as exc:
        log("")
        log(f"[connector] FATAL: could not create its own objects: {exc}")
        log(f"[connector] Does {JOURNAL_DB}.{JOURNAL_SCHEMA} exist with the three Iceberg")
        log("[connector] defaults set? That is Part 1 -- run solutions/01_environment.sql.")
        log("")
        raise SystemExit(3) from exc
    finally:
        cn.close()


class CdcSimulator:
    """Generates the change feed: inserts, re-inspection updates, and voids.

    Connector semantics reproduced here:
      * an UPDATE carries the OLD key in PRIMARY_KEY__* and the NEW values in
        PAYLOAD__*
      * a DELETE carries the key only -- every PAYLOAD__* is NULL
      * deletes are soft; the destination row is flagged, never removed
      * ordering is the (MOST_, LEAST_SIGNIFICANT_POSITION) tuple, monotonic
    """

    def __init__(self, args: Any, sink: Any) -> None:
        self.args = args
        self.sink = sink
        self.rng = random.Random(args.seed)
        self.frame_seq = 0
        self.rows: dict[str, Any] = {}  # inspection_id -> current row state
        self.recent_fails: list[str] = []  # inspection_ids eligible for re-inspection
        self.recent_scans: list[str] = []  # inspection_ids eligible for voiding
        self.incident_until = None
        # Runtime state, not a startup flag: the control table can turn this on and
        # off while the producer keeps streaming. See control_loop().
        #
        # Time-boxed, exactly like the incident. Inspectors clear a backlog and then
        # go back to their normal cadence; they do not re-check every failure for
        # ever. Left latched on, the burst drains the backlog completely and yield
        # pins at 100% with an empty DEFECT_COUNTS_5MIN, which is both unbelievable
        # and less instructive than a visible jump followed by normal operation.
        self.reinspect_until = (
            utcnow() + timedelta(minutes=REINSPECT_MINUTES) if args.reinspect else None
        )
        self.reinspect_quota = 0
        self.counts: dict[str, int] = {"insert": 0, "update": 0, "delete": 0}
        # Logical WAL clock. batch = transaction, msg = position within it.
        self.batch = 0

    # -- incident control ---------------------------------------------------
    def defect_rate(self, line: str) -> float:
        if self.incident_active() and line in INCIDENT_DEFECT_RATE:
            return INCIDENT_DEFECT_RATE[line]
        return BASE_DEFECT_RATE[line]

    def incident_active(self) -> bool:
        return self.incident_until is not None and utcnow() < self.incident_until

    def start_incident(self, minutes: float) -> None:
        self.incident_until = utcnow() + timedelta(minutes=minutes)
        log(f"[cdc] PAINT defect rate -> {INCIDENT_DEFECT_RATE['PAINT']:.0%} for {minutes} min")

    def reinspect_active(self) -> bool:
        return self.reinspect_until is not None and utcnow() < self.reinspect_until

    def start_reinspect(self, minutes: float) -> None:
        self.reinspect_until = utcnow() + timedelta(minutes=minutes)
        # Bounded at burst start: inspectors work through the backlog that exists
        # now, and overturn the share of it that was a false reject.
        self.reinspect_quota = max(1, int(len(self.recent_fails) * REINSPECT_FRACTION))
        log(
            f"[cdc] inspectors re-checking {self.reinspect_quota} failed units "
            f"over the next {minutes:g} min"
        )

    def stop_reinspect(self) -> None:
        if self.reinspect_until is not None:
            self.reinspect_until = None
            self.reinspect_quota = 0
            log("[cdc] re-inspection back to normal cadence")

    def stop_incident(self) -> None:
        if self.incident_until is not None:
            self.incident_until = None
            log(f"[cdc] PAINT defect rate -> {BASE_DEFECT_RATE['PAINT']:.0%} (back to normal)")

    # -- generation ---------------------------------------------------------
    def new_scan(self) -> dict[str, Any]:
        self.frame_seq += 1
        line = self.rng.choices(LINES, weights=[34, 33, 33])[0]
        failed = self.rng.random() < self.defect_rate(line)

        if failed:
            if self.incident_active() and line == "PAINT":
                # Skew hard to one code so "which defect is driving scrap" has an answer.
                defect = (
                    INCIDENT_DEFECT_CODE
                    if self.rng.random() < 0.8
                    else self.rng.choice(DEFECTS_BY_LINE[line])
                )
            else:
                defect = self.rng.choice(DEFECTS_BY_LINE[line])
        else:
            defect = None

        now = utcnow()
        return {
            "INSPECTION_ID": f"S-{self.rng.getrandbits(48):012x}",
            "UNIT_ID": f"F-{self.frame_seq:06d}",
            "LINE": line,
            "SKU": self.rng.choice(SKUS),
            "STATUS": "FAIL" if failed else "PASS",
            "DEFECT_CODE": defect,
            "STATION_ID": STATION_BY_LINE[line],
            "OPERATOR_ID": self.rng.choice(OPERATORS),
            "EVENT_TS": now,
            "UPDATED_TS": now,
        }

    def tick(self, n: int) -> None:
        """One transaction: n inserts, plus any updates and deletes now due."""
        self.batch += 1
        msn = self.batch * 10_000  # end-of-transaction LSN
        msg = 0

        def next_lsn() -> int:
            nonlocal msg
            msg += 1
            return msn + msg

        rows = [self.new_scan() for _ in range(n)]
        fails_this_tick = 0
        for r in rows:
            self.rows[r["INSPECTION_ID"]] = r
            self.sink.emit_insert(r, msn, next_lsn())
            self.counts["insert"] += 1
            self.recent_scans.append(r["INSPECTION_ID"])
            if r["STATUS"] == "FAIL":
                self.recent_fails.append(r["INSPECTION_ID"])
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
        if self.reinspect_active() and self.reinspect_quota > 0:
            # Burst mode: overturn a bounded share of the backlog so yield visibly
            # recovers WITHOUT reaching 100%. Some frames really are scrap, and a
            # bucket that corrects to a perfect score is neither believable nor
            # instructive -- it also empties DEFECT_COUNTS_5MIN, removing the
            # evidence the agent needs. One per tick keeps the correction visible
            # over ~20-30s rather than instant.
            n_upd = 1
            self.reinspect_quota -= 1
        else:
            n_upd = self._poisson_ish(fails_this_tick * self.args.update_rate)

        for _ in range(n_upd):
            if not self.recent_fails:
                break
            inspection_id = self.recent_fails.pop(self.rng.randrange(len(self.recent_fails)))
            row = self.rows.get(inspection_id)
            if row is None:
                continue
            updated = copy.copy(row)
            updated["STATUS"] = "PASS"
            updated["DEFECT_CODE"] = None
            updated["UPDATED_TS"] = utcnow()
            self.rows[inspection_id] = updated
            # old key, new payload -- the connector's UPDATE shape
            self.sink.emit_update(inspection_id, updated, msn, next_lsn())
            self.counts["update"] += 1

        # DELETE: a duplicate barcode scan is voided. Soft delete downstream.
        n_del = self._poisson_ish(len(rows) * self.args.delete_rate)
        for _ in range(n_del):
            if not self.recent_scans:
                break
            inspection_id = self.recent_scans.pop(self.rng.randrange(len(self.recent_scans)))
            if inspection_id not in self.rows:
                continue
            self.sink.emit_delete(inspection_id, msn, next_lsn())
            self.rows.pop(inspection_id, None)
            self.counts["delete"] += 1

        self.sink.tick_done()

    def _poisson_ish(self, expected: float) -> int:
        base = int(expected)
        return base + (1 if self.rng.random() < (expected - base) else 0)


class CdcSink:
    def emit_insert(self, row: dict[str, Any], msn: int, lsn: int) -> None:
        raise NotImplementedError

    def emit_update(self, old_key: str, new_row: dict[str, Any], msn: int, lsn: int) -> None:
        raise NotImplementedError

    def emit_delete(self, key: str, msn: int, lsn: int) -> None:
        raise NotImplementedError

    def tick_done(self) -> None:
        pass

    def close(self) -> None:
        pass


def journal_event(
    pk: str,
    event_type: str,
    row: dict[str, Any] | None,
    msn: int,
    lsn: int,
) -> dict[str, Any]:
    """The connector's flat 'Snowflake Journal' wire shape.

    PRIMARY_KEY__* is the OLD key (identical to the new one here -- this lab's
    replication key is immutable). On DELETE every PAYLOAD__* is NULL.
    """
    ev: dict[str, Any] = {
        "PRIMARY_KEY__INSPECTION_ID": pk,
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

    def __init__(self, profile_path: str, profile: dict[str, Any], merge: bool = True) -> None:
        from snowflake.ingest.streaming import StreamingIngestClient

        self.client = StreamingIngestClient(
            client_name="cascade_cdc_journal",
            db_name=JOURNAL_DB,
            schema_name=JOURNAL_SCHEMA,
            pipe_name=f"{JOURNAL_TABLE}-STREAMING",
            profile_json=profile_path,
        )
        # HTTP 409 / ERR_CHANNEL_HAS_UNCOMMITTED_DATA: a prior run did not flush
        # cleanly. Wait ~30s for the SDK to reconcile, then restart the producer.
        self.channel, _ = self.client.open_channel(channel_name="cdc_journal_1")

        # Separate SQL connection for the merge, exactly as the connector has.
        self.cn = None
        self.merges = 0
        self.rows_merged = 0
        if merge:
            self.cn = connect_sql(profile, query_tag=MERGE_QUERY_TAG)
            self.lock = threading.Lock()

    def _send(self, ev: dict[str, Any], lsn: int) -> None:
        self.channel.append_row(ev, offset_token=str(lsn))

    def emit_insert(self, row: dict[str, Any], msn: int, lsn: int) -> None:
        self._send(journal_event(row["INSPECTION_ID"], EV_INSERT, row, msn, lsn), lsn)

    def emit_update(self, old_key: str, new_row: dict[str, Any], msn: int, lsn: int) -> None:
        self._send(journal_event(old_key, EV_UPDATE, new_row, msn, lsn), lsn)

    def emit_delete(self, key: str, msn: int, lsn: int) -> None:
        self._send(journal_event(key, EV_DELETE, None, msn, lsn), lsn)

    def run_merge(self) -> tuple[int, float]:
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

    def close(self) -> None:
        try:
            self.channel.close()
            self.client.close()
        except Exception as exc:  # SDK teardown can raise various types; log and continue
            log(f"[cdc journal] channel/client close error (ignored): {exc}")
        try:
            if self.cn is not None:
                self.cn.close()
        except Exception as exc:  # same rationale
            log(f"[cdc journal] connector close error (ignored): {exc}")


INSERT_SQL = f"""
INSERT INTO {SCANS_TABLE}
  (INSPECTION_ID, UNIT_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID, OPERATOR_ID,
   EVENT_TS, UPDATED_TS, _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

UPDATE_SQL = f"""
UPDATE {SCANS_TABLE}
   SET STATUS = %s, DEFECT_CODE = %s, UPDATED_TS = %s, _SNOWFLAKE_UPDATED_AT = %s
 WHERE INSPECTION_ID = %s AND _SNOWFLAKE_DELETED = FALSE
"""

VOID_SQL = f"""
UPDATE {SCANS_TABLE}
   SET _SNOWFLAKE_DELETED = TRUE, _SNOWFLAKE_UPDATED_AT = %s
 WHERE INSPECTION_ID = %s
"""


class DirectDmlSink(CdcSink):
    """Fallback path: writes the settled result the MERGE would have produced.

    Skips the journal, the stream and the task entirely, so it loses the
    observable ~60s merge gate and the two-path design -- but it needs no CDC
    objects and lands rows in a second.
    """

    def __init__(self, profile: dict[str, Any]) -> None:
        self.cn = connect_sql(profile)
        self.lock = threading.Lock()
        self.pending: list[Any] = []

    def emit_insert(self, row: dict[str, Any], _msn: int, _lsn: int) -> None:
        now = utcnow()
        self.pending.append(
            (
                row["INSPECTION_ID"],
                row["UNIT_ID"],
                row["LINE"],
                row["SKU"],
                row["STATUS"],
                row["DEFECT_CODE"],
                row["STATION_ID"],
                row["OPERATOR_ID"],
                row["EVENT_TS"],
                row["UPDATED_TS"],
                now,
                now,
                False,
            )
        )

    def emit_update(self, old_key: str, new_row: dict[str, Any], _msn: int, _lsn: int) -> None:
        self._exec(
            UPDATE_SQL,
            (
                new_row["STATUS"],
                new_row["DEFECT_CODE"],
                new_row["UPDATED_TS"],
                utcnow(),
                old_key,
            ),
        )

    def emit_delete(self, key: str, _msn: int, _lsn: int) -> None:
        self._exec(VOID_SQL, (utcnow(), key))

    def tick_done(self) -> None:
        if not self.pending:
            return
        batch, self.pending = self.pending, []
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.executemany(INSERT_SQL, batch)
            finally:
                cur.close()

    def _exec(self, sql: str, params: tuple[Any, ...]) -> None:
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.execute(sql, params)
            finally:
                cur.close()

    def close(self) -> None:
        try:
            self.tick_done()
            self.cn.close()
        except Exception as exc:  # flush/close on teardown; log and continue
            log(f"[cdc direct] close error (ignored): {exc}")


class DryRunCdcSink(CdcSink):
    def __init__(self, mode: str) -> None:
        self.mode = mode

    def _out(self, obj: Any) -> None:
        sys.stdout.write(json.dumps(obj, default=str) + "\n")

    def emit_insert(self, row: dict[str, Any], msn: int, lsn: int) -> None:
        self._out(
            journal_event(row["INSPECTION_ID"], EV_INSERT, row, msn, lsn)
            if self.mode == "journal"
            else {"op": "INSERT", **row}
        )

    def emit_update(self, old_key: str, new_row: dict[str, Any], msn: int, lsn: int) -> None:
        self._out(
            journal_event(old_key, EV_UPDATE, new_row, msn, lsn)
            if self.mode == "journal"
            else {"op": "UPDATE", **new_row}
        )

    def emit_delete(self, key: str, msn: int, lsn: int) -> None:
        self._out(
            journal_event(key, EV_DELETE, None, msn, lsn)
            if self.mode == "journal"
            else {"op": "DELETE(soft)", "INSPECTION_ID": key}
        )


def merge_loop(sink: JournalSink, gate_seconds: float) -> None:
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
        except Exception as exc:  # keep loop alive; merge errors are typically transient
            log(f"[merge] error: {exc}")
            continue
        if affected:
            log(
                f"[merge] gate fired: {affected} rows applied in {secs:.1f}s "
                f"(merges={sink.merges} rows_total={sink.rows_merged})"
            )
        else:
            # Nothing queued. The connector yields here rather than merging.
            log("[merge] gate fired: nothing queued, skipped")


def cdc_loop(sim: CdcSimulator, rate: float, status_every: float = 15.0) -> None:
    last_status = time.time()
    while not _stop.is_set():
        started = time.time()
        try:
            sim.tick(max(1, int(round(rate))))
        except Exception as exc:  # keep loop alive; tick errors are typically transient
            log(f"[cdc] error: {exc}")
            time.sleep(2.0)
            continue
        if time.time() - last_status >= status_every:
            c = sim.counts
            log(
                f"[cdc] inserts={c['insert']} updates={c['update']} "
                f"soft_deletes={c['delete']}"
                f"{'  INCIDENT' if sim.incident_active() else ''}"
            )
            last_status = time.time()
        _stop.wait(max(0.0, 1.0 - (time.time() - started)))
