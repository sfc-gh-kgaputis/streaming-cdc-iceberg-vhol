"""The station telemetry feed: sensors streaming straight into Iceberg.

This half is NOT Openflow. It is a plain Snowpipe Streaming client writing to a
Snowflake-managed Iceberg table, so its target table is created directly rather
than provisioned by the simulated connector.
"""

from __future__ import annotations

import json
import random
import sys
import time
from typing import Any

from common import (
    STATION_BY_LINE,
    TELEMETRY_DB,
    TELEMETRY_SCHEMA,
    TELEMETRY_TABLE,
    _stop,
    iso,
    log,
    utcnow,
)

# Telemetry metrics: healthy centre and jitter.
METRICS = {
    "weld_current": ("WELD", 185.0, 4.0),
    "booth_humidity": ("PAINT", 44.0, 1.5),
    "booth_temp": ("PAINT", 22.5, 0.6),
    "torque_nm": ("ASSEMBLY", 12.0, 0.4),
}

# Humidity target once the drift kicks in. Cause precedes effect.
INCIDENT_HUMIDITY = 71.0


class TelemetrySimulator:
    def __init__(self, args: Any, sink: Any) -> None:
        self.args = args
        self.sink = sink
        self.rng = random.Random((args.seed or 0) + 7)
        self.drift_start = None
        self.ramp = 1.0

    def start_drift(self, ramp_seconds: float) -> None:
        self.drift_start = utcnow()
        self.ramp = max(1.0, ramp_seconds)
        log(
            f"[telem] booth_humidity ramping {METRICS['booth_humidity'][1]:.0f} -> "
            f"{INCIDENT_HUMIDITY:.0f} over {ramp_seconds:.0f}s"
        )

    def stop_drift(self) -> None:
        if self.drift_start is not None:
            self.drift_start = None
            log(f"[telem] booth_humidity -> {METRICS['booth_humidity'][1]:.0f} (booth fixed)")

    def current_humidity(self) -> float:
        base = METRICS["booth_humidity"][1]
        if self.drift_start is None:
            return base
        frac = min(1.0, (utcnow() - self.drift_start).total_seconds() / self.ramp)
        return base + (INCIDENT_HUMIDITY - base) * frac

    def batch(self, n: int) -> list[dict[str, Any]]:
        rows = []
        for _ in range(n):
            metric = self.rng.choice(list(METRICS))
            line, centre, jitter = METRICS[metric]
            if metric == "booth_humidity":
                centre = self.current_humidity()
            rows.append(
                {
                    "STATION_ID": STATION_BY_LINE[line],
                    "LINE": line,
                    "METRIC": metric,
                    "VALUE": round(self.rng.gauss(centre, jitter), 3),
                    "EVENT_TS": iso(utcnow()),
                }
            )
        return rows


class TelemetrySink:
    def __init__(self, profile_path: str) -> None:
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

    def send(self, rows: list[dict[str, Any]]) -> None:
        for r in rows:
            self.channel.append_row(r, offset_token=str(self.offset))
            self.offset += 1

    def close(self) -> None:
        try:
            self.channel.close()
            self.client.close()
        except Exception as exc:  # SDK teardown can raise various types; log and continue
            log(f"[telem] channel/client close error (ignored): {exc}")


class DryRunTelemetrySink:
    def send(self, rows: list[dict[str, Any]]) -> None:
        for r in rows:
            sys.stdout.write("TELEM " + json.dumps(r) + "\n")

    def close(self) -> None:
        pass


def telemetry_loop(
    sim: TelemetrySimulator, sink: Any, rate: float, status_every: float = 15.0
) -> None:
    sent = 0
    last_status = time.time()
    while not _stop.is_set():
        started = time.time()
        try:
            rows = sim.batch(max(1, int(round(rate))))
            sink.send(rows)
            sent += len(rows)
        except Exception as exc:  # keep loop alive; batch errors are typically transient
            log(f"[telem] error: {exc}")
            time.sleep(2.0)
            continue
        if time.time() - last_status >= status_every:
            log(f"[telem] rows={sent} booth_humidity~{sim.current_humidity():.1f}")
            last_status = time.time()
        _stop.wait(max(0.0, 1.0 - (time.time() - started)))
