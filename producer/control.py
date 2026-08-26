"""The simulator's control plane.

The attendee changes the WORLD by writing a row to a control table; the pipeline is
never restarted. A real Openflow connector runs continuously -- an incident changes
the character of the data at the source, it does not bounce the connector.
"""

from __future__ import annotations

import threading
from typing import Any

from common import (
    CONTROL_POLL_SECONDS,
    CONTROL_TABLE,
    REINSPECT_MINUTES,
    _stop,
    connect_sql,
    log,
)


def arm_incident(cdc_sim: Any, telem_sim: Any, after: float, minutes: float) -> None:
    """The two-phase cascade: cause leads effect.

    Humidity starts drifting immediately; PAINT defects spike `after` seconds
    later. The agent has to notice the cause preceded the effect, so the delay
    is deliberate, not an artefact of how fast the pipeline is.
    """
    if telem_sim:
        telem_sim.start_drift(after)
    if cdc_sim:

        def arm() -> None:
            if not _stop.wait(after):
                cdc_sim.start_incident(minutes)

        threading.Thread(target=arm, daemon=True, name="arm").start()


def apply_mode(mode: str, cdc_sim: Any, telem_sim: Any, args: Any) -> None:
    """Move the simulated world into `mode`. The pipeline is not touched."""
    if mode == "INCIDENT":
        log("[control] mode -> INCIDENT: booth humidity climbing, defects to follow")
        arm_incident(cdc_sim, telem_sim, args.incident_after, args.incident_minutes)
    elif mode == "REINSPECT":
        log("[control] mode -> REINSPECT: booth fixed, inspectors re-checking failed units")
        if telem_sim:
            telem_sim.stop_drift()
        if cdc_sim:
            cdc_sim.stop_incident()
            cdc_sim.start_reinspect(REINSPECT_MINUTES)
    elif mode == "STEADY":
        log("[control] mode -> STEADY")
        if telem_sim:
            telem_sim.stop_drift()
        if cdc_sim:
            cdc_sim.stop_incident()
            cdc_sim.stop_reinspect()
    else:
        log(f"[control] unknown mode {mode!r} -- ignoring, staying as we are")


def control_loop(profile: dict[str, Any], cdc_sim: Any, telem_sim: Any, args: Any) -> None:
    """Poll the control table and move the world when it changes.

    This is why the producer never restarts. A real Openflow connector runs
    continuously; an incident changes the data at the SOURCE, it does not bounce
    the connector. Restarting to change modes would also mean reopening a channel
    name within seconds of closing it, which is exactly what raises
    ERR_CHANNEL_HAS_UNCOMMITTED_DATA (HTTP 409).

    A missing or unreadable control table is not fatal -- the producer keeps
    streaming in whatever mode it started in, and says so once.
    """
    try:
        cn = connect_sql(profile)
    except Exception as exc:
        log(f"[control] disabled, could not connect: {exc}")
        return

    current: str | None = None
    try:
        while not _stop.is_set():
            if _stop.wait(CONTROL_POLL_SECONDS):
                break
            try:
                cur = cn.cursor()
                cur.execute(
                    f"SELECT MODE FROM {CONTROL_TABLE} ORDER BY UPDATED_AT DESC LIMIT 1"
                )
                row = cur.fetchone()
                cur.close()
            except Exception as exc:
                log(f"[control] read failed, staying in {current or 'startup'} mode: {exc}")
                continue
            mode = (row[0] if row and row[0] else "STEADY").strip().upper()
            if mode == current:
                continue
            current = mode
            apply_mode(mode, cdc_sim, telem_sim, args)
    finally:
        cn.close()
