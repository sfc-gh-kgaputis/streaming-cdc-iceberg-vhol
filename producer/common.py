"""Shared plumbing: logging, time, the pinned object names, and credentials.

Everything in here is used by more than one of the other modules. If something is
only used by one, it belongs in that module instead.
"""

from __future__ import annotations

import json
import pathlib
import sys
import threading
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Plant model shared by both feeds
# ---------------------------------------------------------------------------
LINES = ["WELD", "PAINT", "ASSEMBLY"]

# One station per line, so a scan and a sensor reading agree on where they happened.
# That shared key is what lets the Gold layer join the two feeds at all.
STATION_BY_LINE = {
    "WELD": "ST-WELD-01",
    "PAINT": "ST-PAINT-01",
    "ASSEMBLY": "ST-ASSY-01",
}

# ---------------------------------------------------------------------------
# Object names
# ---------------------------------------------------------------------------
SCANS_TABLE = "MFG.RAW.QUALITY_INSPECTIONS"

# The connector names its journal "<TABLE>_JOURNAL_<series>_<generation>", where
# series is epoch seconds at table registration and generation starts at 1 and
# increments on every schema change. We PIN the series so the lab has stable
# object names the skill can reference; in production it is not predictable.
JOURNAL_SERIES = "1787700000"
JOURNAL_GENERATION = "1"
JOURNAL_TABLE = f"QUALITY_INSPECTIONS_JOURNAL_{JOURNAL_SERIES}_{JOURNAL_GENERATION}"

TELEMETRY_DB, TELEMETRY_SCHEMA, TELEMETRY_TABLE = "MFG", "RAW", "STATION_TELEMETRY"
JOURNAL_DB, JOURNAL_SCHEMA = "MFG", "RAW"

# The simulator's control plane. The attendee changes the WORLD by writing here;
# the pipeline never restarts. A real Openflow connector runs continuously and an
# incident changes the character of the data at the source -- it does not bounce
# the connector. Deliberately a standard table: it is operational metadata, not a
# feed and not derived, so making it Iceberg would buy nothing and add one more
# place for the ICEBERG_VERSION_DEFAULT session-schema trap to bite.
CONTROL_TABLE = f"{JOURNAL_DB}.{JOURNAL_SCHEMA}.SIMULATOR_CONTROL"

# Polled often enough to feel instant on stage. This costs no extra warehouse
# time: the merge gate already runs a query every 60s against a warehouse whose
# AUTO_SUSPEND is 60s, so HOL_WH is awake for as long as the producer runs anyway.
CONTROL_POLL_SECONDS = 10.0

# How long the re-inspection burst runs. Long enough to visibly clear the backlog
# across a 5-minute bucket or two, short enough that the plant returns to a
# believable steady state instead of a permanent 100% yield.
REINSPECT_MINUTES = 3.0

# Share of the failed backlog the inspectors overturn. Deliberately well under
# 1.0: a corrected bucket should visibly improve, not become perfect.
REINSPECT_FRACTION = 0.4


_stop = threading.Event()


def log(msg: str) -> None:
    """Progress goes to stderr so it does not flood a chat transcript when the
    producer runs in the background."""
    print(msg, file=sys.stderr, flush=True)


def utcnow() -> datetime:
    """Naive UTC. The account is UTC and Iceberg TIMESTAMP_NTZ wants no offset."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso(ts: Any) -> Any:
    return ts.isoformat(timespec="milliseconds") if isinstance(ts, datetime) else ts



# The lab identity is pinned, not discovered. A PAT is bound to the user it was
# minted for, so if profile.json names anyone else, authentication fails with an
# error that mentions neither the user nor the token. Observed 26 Aug: a Desktop
# run wrote the *connected* user (the signup admin) into profile.json instead of
# the lab user, because CURRENT_USER() was queried rather than set literally.
LAB_USER = "HOL_USER"


def _credential_works(profile: dict[str, Any]) -> bool:
    """Can we actually authenticate with this profile? One cheap round trip."""
    try:
        cn = connect_sql(profile)
    except Exception:
        return False
    try:
        cur = cn.cursor()
        cur.execute("SELECT CURRENT_USER()")
        return (cur.fetchone() or [None])[0] is not None
    except Exception:
        return False
    finally:
        cn.close()


def repair_profile(profile_path: str, verify: bool = True) -> dict[str, Any]:
    """Verify profile.json, and repair it ONLY if a candidate fix actually works.

    Deliberately never repairs blind. An earlier version pinned the user and took
    `secret.pat` as the source of truth unconditionally, which is wrong in both
    directions: it would clobber a deliberately different user, and -- worse -- a
    stale `secret.pat` would overwrite a *working* token and break a setup that was
    fine. So the order is: try what is on disk, and only change it if a candidate
    both differs and authenticates.

    The Snowpipe Streaming SDK reads this file itself (`profile_json=`), so a fix
    has to land on disk; pinning values in Python is not enough.
    """
    path = pathlib.Path(profile_path)
    profile = json.loads(path.read_text())

    if not verify:
        return profile

    if _credential_works(profile):
        return profile

    log("[profile] cannot authenticate with profile.json as written -- trying known fixes")

    # Candidate repairs, narrowest first. Each is only accepted if it authenticates.
    candidates: list[tuple[str, dict[str, Any]]] = []

    if profile.get("user") != LAB_USER:
        c = dict(profile)
        c["user"] = LAB_USER
        candidates.append((f"user {profile.get('user')!r} -> {LAB_USER!r}", c))

    secret = path.parent.parent / "secret.pat"
    if secret.exists():
        token = secret.read_text().strip()
        if token and token != profile.get("personal_access_token"):
            c = dict(profile)
            c["personal_access_token"] = token
            candidates.append(("token -> the one in secret.pat", c))
            if profile.get("user") != LAB_USER:
                c2 = dict(c)
                c2["user"] = LAB_USER
                candidates.append((f"user -> {LAB_USER!r} AND token -> secret.pat", c2))

    for label, candidate in candidates:
        if _credential_works(candidate):
            path.write_text(json.dumps(candidate, indent=2) + "\n")
            log("")
            log("[profile] WARNING: profile.json was wrong and has been REPAIRED IN PLACE.")
            log(f"[profile] WARNING: changed {label}.")
            log("[profile] WARNING: re-run Setup D if the producer fails again.")
            log("")
            return candidate

    # Nothing worked. Say precisely what was tried, because the SDK's own error
    # will not mention the user, the token or the account.
    log("")
    log("[profile] FATAL: could not authenticate, and no known repair worked.")
    log(f"[profile]   account : {profile.get('account')}")
    log(f"[profile]   user    : {profile.get('user')}  (the PAT must belong to this user)")
    log(f"[profile]   token   : {'present' if profile.get('personal_access_token') else 'MISSING'}"
        f" in profile.json, secret.pat {'exists' if secret.exists() else 'MISSING'}")
    log("[profile] Check, in order:")
    log("[profile]   1. secret.pat holds the token_secret from Setup B, whole and unwrapped")
    log(f"[profile]   2. that token was minted for {LAB_USER}, not for your signup admin")
    log("[profile]   3. it has not expired -- tokens last 7 days; re-mint or ROTATE in Snowsight")
    log("[profile]   4. account is your trial account, not another one you have a connection to")
    log("")
    raise SystemExit(2)


def connect_sql(profile: dict[str, Any], query_tag: str | None = None) -> Any:
    """A plain SQL connection, as the connector keeps alongside its stream.

    Snowpipe Streaming does not use a warehouse; every SQL path here does. Kept in
    one place because the merge, the DML fallback and the control poll all need
    the same connection with the same role, warehouse and PAT auth.
    """
    import snowflake.connector as sc

    params = {"QUERY_TAG": query_tag} if query_tag else None
    return sc.connect(
        account=profile["account"],
        user=profile["user"],
        password=profile["personal_access_token"],
        role="ACCOUNTADMIN",
        warehouse="HOL_WH",
        database=JOURNAL_DB,
        schema=JOURNAL_SCHEMA,
        client_session_keep_alive=True,
        session_parameters=params,
    )
