"""Entry point for the Cascade Cycleworks data producer.

Two independent feeds into one Snowflake account:

  --cdc        the simulated Openflow Postgres CDC connector (cdc_simulator.py)
  --telemetry  station sensors over Snowpipe Streaming (telemetry.py)

Start it ONCE, in Part 2, and leave it running for the rest of the lab. Part 5
changes the plant by writing to MFG.RAW.SIMULATOR_CONTROL (control.py); nothing
here needs restarting.

  python producer/main.py --cdc --telemetry
  python producer/main.py --dry-run --cdc --seed 42      # no account needed
"""

from __future__ import annotations

import argparse
import pathlib
import signal
import threading
from typing import Any

from cdc_simulator import (
    CdcSimulator,
    DirectDmlSink,
    DryRunCdcSink,
    JournalSink,
    cdc_loop,
    ensure_objects,
    merge_loop,
)
from common import JOURNAL_DB, JOURNAL_SCHEMA, JOURNAL_TABLE, _stop, log, repair_profile
from control import arm_incident, control_loop
from telemetry import DryRunTelemetrySink, TelemetrySimulator, TelemetrySink, telemetry_loop


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Cascade Cycleworks plant-floor producer (CDC + telemetry)."
    )
    ap.add_argument(
        "--profile",
        default=str(pathlib.Path(__file__).resolve().parent.parent / "profile.json"),
        help="path to profile.json (default: profile.json in the repo root)",
    )
    ap.add_argument("--cdc", action="store_true", help="run the CDC source")
    ap.add_argument("--telemetry", action="store_true", help="run the telemetry source")
    ap.add_argument(
        "--cdc-mode",
        choices=["journal", "direct"],
        default="journal",
        help="journal = faithful connector path (default); "
        "direct = write the settled result straight to the table",
    )
    ap.add_argument(
        "--merge-gate-seconds",
        type=float,
        default=60.0,
        help="the connector's CRON merge-eligibility gate, in seconds "
        "(default 60, i.e. second :00 of every minute -- this lab's choice, not a "
        "documented connector default). Lower it to watch CDC latency fall, but "
        "not below the ~30s streaming floor.",
    )
    ap.add_argument(
        "--no-merge",
        action="store_true",
        help="write the journal but never merge it, so you can watch the "
        "journal fill while the destination table stays empty",
    )
    ap.add_argument("--rate", type=float, default=2.0, help="scans/sec (default 2)")
    ap.add_argument(
        "--telemetry-rate",
        type=float,
        default=60.0,
        help="telemetry rows/sec (default 60)",
    )
    ap.add_argument(
        "--update-rate",
        type=float,
        default=0.15,
        help="fraction of FAILED frames later overturned to PASS (default 0.15)",
    )
    ap.add_argument(
        "--delete-rate",
        type=float,
        default=0.005,
        help="voided scans as a fraction of inserts (default 0.005)",
    )
    ap.add_argument(
        "--incident",
        action="store_true",
        help="humidity drift, then a PAINT defect spike ~90s later",
    )
    ap.add_argument(
        "--incident-after",
        type=float,
        default=90.0,
        help="seconds of humidity drift before defects spike (default 90)",
    )
    ap.add_argument(
        "--incident-minutes",
        type=float,
        default=20.0,
        help="how long the defect spike lasts (default 20)",
    )
    ap.add_argument(
        "--reinspect",
        action="store_true",
        help="burst of re-inspections: watch yield RECOVER",
    )
    ap.add_argument(
        "--no-control",
        action="store_true",
        help="ignore MFG.RAW.SIMULATOR_CONTROL; use only the flags given here",
    )
    ap.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="stop after N seconds (default: run until Ctrl-C)",
    )
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="print to stdout, no Snowflake connection",
    )
    args = ap.parse_args()

    if not args.cdc and not args.telemetry:
        args.cdc = args.telemetry = True

    profile = None
    if not args.dry_run:
        profile = repair_profile(args.profile)

    def handle_stop(_sig: Any, _frm: Any) -> None:
        log("stopping...")
        _stop.set()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    threads: list[threading.Thread] = []
    sinks: list[Any] = []
    cdc_sim = telem_sim = None

    if args.cdc:
        if args.dry_run:
            sink: Any = DryRunCdcSink(args.cdc_mode)
        elif args.cdc_mode == "journal":
            # The connector provisions its own targets first. Has to happen before the
            # sink opens a channel, because the pipe is derived from the journal table.
            ensure_objects(profile)
            sink = JournalSink(args.profile, profile, merge=not args.no_merge)
        else:
            sink = DirectDmlSink(profile)
        sinks.append(sink)
        cdc_sim = CdcSimulator(args, sink)
        threads.append(
            threading.Thread(target=cdc_loop, args=(cdc_sim, args.rate), daemon=True, name="cdc")
        )
        # The merge processor is part of the connector, so it runs here -- not as
        # a Snowflake task. See merge_loop().
        if args.cdc_mode == "journal" and not args.dry_run and not args.no_merge:
            threads.append(
                threading.Thread(
                    target=merge_loop,
                    args=(sink, args.merge_gate_seconds),
                    daemon=True,
                    name="merge",
                )
            )

    if args.telemetry:
        sink = DryRunTelemetrySink() if args.dry_run else TelemetrySink(args.profile)
        sinks.append(sink)
        telem_sim = TelemetrySimulator(args, sink)
        threads.append(
            threading.Thread(
                target=telemetry_loop,
                args=(telem_sim, sink, args.telemetry_rate),
                daemon=True,
                name="telem",
            )
        )

    log(
        f"starting: cdc={args.cdc} ({args.cdc_mode}) telemetry={args.telemetry} "
        f"scans/s={args.rate} telem/s={args.telemetry_rate}"
    )
    if args.cdc and args.cdc_mode == "journal" and not args.dry_run:
        log(f"[cdc] journal: {JOURNAL_DB}.{JOURNAL_SCHEMA}.{JOURNAL_TABLE}")
        if args.no_merge:
            log("[merge] DISABLED (--no-merge): the destination table will not change")
        else:
            log(
                f"[merge] this process issues the MERGE itself, on a "
                f"{args.merge_gate_seconds:.0f}s CRON gate -- the connector does not "
                f"create a Snowflake task"
            )

    for t in threads:
        t.start()

    # --incident / --reinspect still work for a presenter testing from a shell, but
    # the lab drives both from the control table so the producer never restarts.
    if args.incident:
        arm_incident(cdc_sim, telem_sim, args.incident_after, args.incident_minutes)

    if not args.dry_run and not args.no_control:
        threading.Thread(
            target=control_loop,
            args=(profile, cdc_sim, telem_sim, args),
            daemon=True,
            name="control",
        ).start()

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
            log(
                f"final cdc: inserts={c['insert']} updates={c['update']} soft_deletes={c['delete']}"
            )
        log("stopped")


if __name__ == "__main__":
    main()
