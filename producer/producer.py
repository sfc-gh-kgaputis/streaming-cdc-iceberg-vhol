#!/usr/bin/env python3
"""Cascade Cycleworks plant-floor data producer.

Two sources in one process, mirroring what runs in production:

  --cdc         Stands in for the Openflow Postgres CDC connector. Maintains
                MFG.CDC.PRODUCTION_SCANS with INSERTs, UPDATEs (re-inspection)
                and soft DELETEs, including the connector's _SNOWFLAKE_* columns.

  --telemetry   Real Snowpipe Streaming into the Iceberg table
                MFG.RAW.STATION_TELEMETRY. This is genuine high-throughput
                streaming ingest, not a simulation.

Only the *connector* is simulated. Everything downstream is real.

Examples
--------
  # steady state, both sources
  python producer/producer.py --profile producer/profile.json --cdc --telemetry

  # the incident: humidity climbs in the paint booth, then PAINT defects spike
  python producer/producer.py --profile producer/profile.json --cdc --telemetry --incident

  # the recovery: inspectors re-check failed frames, FAIL -> PASS
  python producer/producer.py --profile producer/profile.json --cdc --telemetry --reinspect

  # no Snowflake account needed, prints what it would send
  python producer/producer.py --dry-run --cdc
"""

import argparse
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

SCANS_TABLE = "MFG.CDC.PRODUCTION_SCANS"
TELEMETRY_DB, TELEMETRY_SCHEMA, TELEMETRY_TABLE = "MFG", "RAW", "STATION_TELEMETRY"

_stop = threading.Event()


def utcnow():
    """Naive UTC. The account is set to UTC, and Iceberg TIMESTAMP_NTZ wants no offset."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso(ts):
    return ts.isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# CDC source: stands in for the Openflow connector
# ---------------------------------------------------------------------------
class CdcSimulator:
    """Generates scan INSERTs plus the UPDATE and soft-DELETE traffic that makes
    this a change feed rather than an append-only log.

    The connector's contract we reproduce:
      * _SNOWFLAKE_INSERTED_AT / _SNOWFLAKE_UPDATED_AT are connector-maintained
      * DELETE is a soft delete: _SNOWFLAKE_DELETED = TRUE, row retained
    """

    def __init__(self, args, sink):
        self.args = args
        self.sink = sink
        self.rng = random.Random(args.seed)
        self.frame_seq = 0
        # Recent FAILs are re-inspection candidates; recent scans are void candidates.
        self.recent_fails = []
        self.recent_scans = []
        self.incident_until = None
        self.counts = {"insert": 0, "update": 0, "delete": 0}

    def defect_rate(self, line):
        if self.incident_active() and line in INCIDENT_DEFECT_RATE:
            return INCIDENT_DEFECT_RATE[line]
        return BASE_DEFECT_RATE[line]

    def incident_active(self):
        return self.incident_until is not None and utcnow() < self.incident_until

    def start_incident(self, minutes):
        self.incident_until = utcnow() + timedelta(minutes=minutes)
        print(f"[cdc] PAINT defect rate -> {INCIDENT_DEFECT_RATE['PAINT']:.0%} "
              f"for {minutes} min", flush=True)

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
            "_SNOWFLAKE_INSERTED_AT": now,
            "_SNOWFLAKE_UPDATED_AT": now,
            "_SNOWFLAKE_DELETED": False,
        }

    def tick(self, n):
        """One batch: n inserts, plus whatever updates and deletes are due."""
        rows = [self.new_scan() for _ in range(n)]
        self.sink.insert_scans(rows)
        self.counts["insert"] += len(rows)

        fails_this_tick = 0
        for r in rows:
            self.recent_scans.append(r["SCAN_ID"])
            if r["STATUS"] == "FAIL":
                self.recent_fails.append(r["SCAN_ID"])
                fails_this_tick += 1
        # Bound the candidate pools.
        self.recent_scans = self.recent_scans[-4000:]
        self.recent_fails = self.recent_fails[-2000:]

        # UPDATE: an inspector re-checks a failed frame and passes it. This is the
        # case that forces aggregates to DECREASE, which append-only pipelines get wrong.
        #
        # Rate is a fraction of FAILS, not of all scans -- you cannot overturn more
        # frames than actually failed. Expressing it against inserts (as an earlier
        # draft did) silently overturned every failure and pinned yield at 100%.
        if self.args.reinspect:
            # Burst mode: drain the accumulated backlog so yield visibly recovers.
            n_upd = max(1, int(len(self.recent_fails) * 0.10))
        else:
            n_upd = self._poisson_ish(fails_this_tick * self.args.update_rate)

        for _ in range(n_upd):
            if not self.recent_fails:
                break
            scan_id = self.recent_fails.pop(self.rng.randrange(len(self.recent_fails)))
            self.sink.reinspect_pass(scan_id, utcnow())
            self.counts["update"] += 1

        # DELETE: a duplicate barcode scan is voided. Soft delete, per the connector.
        n_del = self._poisson_ish(len(rows) * self.args.delete_rate)
        for _ in range(n_del):
            if not self.recent_scans:
                break
            scan_id = self.recent_scans.pop(self.rng.randrange(len(self.recent_scans)))
            self.sink.void_scan(scan_id, utcnow())
            self.counts["delete"] += 1

    def _poisson_ish(self, expected):
        base = int(expected)
        return base + (1 if self.rng.random() < (expected - base) else 0)


# ---------------------------------------------------------------------------
# Telemetry source: real Snowpipe Streaming
# ---------------------------------------------------------------------------
class TelemetrySimulator:
    def __init__(self, args, sink):
        self.args = args
        self.sink = sink
        self.rng = random.Random((args.seed or 0) + 7)
        self.drift_start = None
        self.humidity = METRICS["booth_humidity"][1]

    def start_drift(self, ramp_seconds):
        self.drift_start = utcnow()
        self.ramp = ramp_seconds
        print(f"[telem] booth_humidity ramping {METRICS['booth_humidity'][1]:.0f} -> "
              f"{INCIDENT_HUMIDITY:.0f} over {ramp_seconds}s", flush=True)

    def current_humidity(self):
        base = METRICS["booth_humidity"][1]
        if self.drift_start is None:
            return base
        elapsed = (utcnow() - self.drift_start).total_seconds()
        frac = min(1.0, elapsed / max(1.0, self.ramp))
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
# Sinks
# ---------------------------------------------------------------------------
INSERT_SQL = f"""
INSERT INTO {SCANS_TABLE}
  (SCAN_ID, FRAME_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID, OPERATOR_ID,
   EVENT_TS, UPDATED_TS, _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Re-inspection: FAIL -> PASS, defect cleared, connector bumps _SNOWFLAKE_UPDATED_AT.
REINSPECT_SQL = f"""
UPDATE {SCANS_TABLE}
   SET STATUS = 'PASS', DEFECT_CODE = NULL, UPDATED_TS = %s, _SNOWFLAKE_UPDATED_AT = %s
 WHERE SCAN_ID = %s AND _SNOWFLAKE_DELETED = FALSE
"""

# Void: soft delete only. The row stays; downstream must filter it.
VOID_SQL = f"""
UPDATE {SCANS_TABLE}
   SET _SNOWFLAKE_DELETED = TRUE, _SNOWFLAKE_UPDATED_AT = %s
 WHERE SCAN_ID = %s
"""


class DryRunSink:
    def insert_scans(self, rows):
        for r in rows:
            out = {k: (iso(v) if isinstance(v, datetime) else v) for k, v in r.items()}
            sys.stdout.write("INSERT " + json.dumps(out) + "\n")

    def reinspect_pass(self, scan_id, ts):
        sys.stdout.write(f"UPDATE {scan_id} FAIL->PASS @ {iso(ts)}\n")

    def void_scan(self, scan_id, ts):
        sys.stdout.write(f"DELETE(soft) {scan_id} @ {iso(ts)}\n")

    def send_telemetry(self, rows):
        for r in rows:
            sys.stdout.write("TELEM " + json.dumps(r) + "\n")

    def close(self):
        pass


class SnowflakeCdcSink:
    """Plain DML over a Snowflake connection, exactly what the connector's MERGE
    leaves behind. Fidelity note: the real connector writes a journal table and
    MERGEs from an APPEND_ONLY stream on a CRON gate; this writes the settled
    result directly."""

    def __init__(self, profile):
        import snowflake.connector as sc

        self.cn = sc.connect(
            account=profile["account"],
            user=profile["user"],
            password=profile["personal_access_token"],
            role="ACCOUNTADMIN",
            warehouse="HOL_WH",
            database="MFG",
            schema="CDC",
            client_session_keep_alive=True,
        )
        self.lock = threading.Lock()

    def insert_scans(self, rows):
        payload = [
            (r["SCAN_ID"], r["FRAME_ID"], r["LINE"], r["SKU"], r["STATUS"], r["DEFECT_CODE"],
             r["STATION_ID"], r["OPERATOR_ID"], r["EVENT_TS"], r["UPDATED_TS"],
             r["_SNOWFLAKE_INSERTED_AT"], r["_SNOWFLAKE_UPDATED_AT"], r["_SNOWFLAKE_DELETED"])
            for r in rows
        ]
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.executemany(INSERT_SQL, payload)
            finally:
                cur.close()

    def reinspect_pass(self, scan_id, ts):
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.execute(REINSPECT_SQL, (ts, ts, scan_id))
            finally:
                cur.close()

    def void_scan(self, scan_id, ts):
        with self.lock:
            cur = self.cn.cursor()
            try:
                cur.execute(VOID_SQL, (ts, scan_id))
            finally:
                cur.close()

    def close(self):
        try:
            self.cn.close()
        except Exception:
            pass


class SnowflakeTelemetrySink:
    """Snowpipe Streaming into the Iceberg table. The SDK uses the default pipe
    Snowflake auto-creates for the table; there is no CREATE PIPE anywhere."""

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

    def send_telemetry(self, rows):
        for r in rows:
            self.channel.append_row(r, offset_token=str(self.offset))
            self.offset += 1

    def close(self):
        try:
            self.channel.close()
            self.client.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------
def cdc_loop(sim, rate, status_every=15.0):
    last_status = time.time()
    while not _stop.is_set():
        started = time.time()
        try:
            sim.tick(max(1, int(round(rate))))
        except Exception as exc:
            print(f"[cdc] error: {exc}", flush=True)
            time.sleep(2.0)
            continue
        if time.time() - last_status >= status_every:
            c = sim.counts
            print(f"[cdc] inserts={c['insert']} updates={c['update']} "
                  f"soft_deletes={c['delete']}"
                  f"{'  INCIDENT' if sim.incident_active() else ''}", flush=True)
            last_status = time.time()
        _stop.wait(max(0.0, 1.0 - (time.time() - started)))


def telemetry_loop(sim, sink, rate, status_every=15.0):
    sent = 0
    last_status = time.time()
    while not _stop.is_set():
        started = time.time()
        try:
            rows = sim.batch(max(1, int(round(rate))))
            sink.send_telemetry(rows)
            sent += len(rows)
        except Exception as exc:
            print(f"[telem] error: {exc}", flush=True)
            time.sleep(2.0)
            continue
        if time.time() - last_status >= status_every:
            print(f"[telem] rows={sent} booth_humidity~{sim.current_humidity():.1f}", flush=True)
            last_status = time.time()
        _stop.wait(max(0.0, 1.0 - (time.time() - started)))


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Cascade Cycleworks plant-floor producer (CDC + telemetry).")
    ap.add_argument("--profile", help="path to profile.json (required unless --dry-run)")
    ap.add_argument("--cdc", action="store_true", help="run the CDC source")
    ap.add_argument("--telemetry", action="store_true", help="run the telemetry source")
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
        print("\nstopping...", flush=True)
        _stop.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    threads, sinks = [], []
    cdc_sim = telem_sim = None

    if args.cdc:
        sink = DryRunSink() if args.dry_run else SnowflakeCdcSink(profile)
        sinks.append(sink)
        cdc_sim = CdcSimulator(args, sink)
        threads.append(threading.Thread(target=cdc_loop, args=(cdc_sim, args.rate),
                                        daemon=True, name="cdc"))

    if args.telemetry:
        sink = DryRunSink() if args.dry_run else SnowflakeTelemetrySink(args.profile)
        sinks.append(sink)
        telem_sim = TelemetrySimulator(args, sink)
        threads.append(threading.Thread(target=telemetry_loop,
                                        args=(telem_sim, sink, args.telemetry_rate),
                                        daemon=True, name="telem"))

    print(f"starting: cdc={args.cdc} telemetry={args.telemetry} "
          f"scans/s={args.rate} telem/s={args.telemetry_rate}", flush=True)
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
            print(f"final cdc: inserts={c['insert']} updates={c['update']} "
                  f"soft_deletes={c['delete']}", flush=True)
        print("stopped", flush=True)


if __name__ == "__main__":
    main()
