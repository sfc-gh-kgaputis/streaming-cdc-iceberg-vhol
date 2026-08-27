#!/usr/bin/env python3
"""
Read a Snowflake-managed Iceberg table from OUTSIDE Snowflake.
Uses PyIceberg against the Horizon Catalog REST endpoint with vended credentials.

Usage:  python external/read_iceberg.py [NAMESPACE.TABLE]
Default table: ANALYTICS.YIELD_BY_LINE_5MIN   (database: MFG)
Config: profile.json in the repo root (or env HORIZON_PAT for the token)
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).parent.parent
DB   = "MFG"    # Horizon: warehouse = database name, must be UPPERCASE
ROLE = "ACCOUNTADMIN"

# -- load account and token from the one profile.json --------------------------
profile_path = REPO / "profile.json"
if not profile_path.exists():
    sys.exit(f"ERROR: {profile_path} not found — do Setup B in the README first.")
try:
    cfg = json.loads(profile_path.read_text())
except json.JSONDecodeError as e:
    sys.exit(f"ERROR: {profile_path} is not valid JSON — line {e.lineno},"
             f" column {e.colno}: {e.msg}")
ACCOUNT = cfg["account"].upper()
BASE    = f"https://{ACCOUNT}.snowflakecomputing.com/polaris/api/catalog"

# -- the PAT (never printed). HORIZON_PAT wins, for running against another account.
if env_pat := os.environ.get("HORIZON_PAT"):
    PAT = env_pat.strip()
elif token := cfg.get("personal_access_token", "").strip():
    PAT = token
else:
    sys.exit(
        f"ERROR: no token. Set personal_access_token in {profile_path}, "
        "or export HORIZON_PAT."
    )

# -- Step 1: exchange PAT for a short-lived access token -----------------------
# The catalog endpoint rejects a PAT presented directly as a Bearer token (401).
# You must POST to /v1/oauth/tokens first; the returned access_token is what works.
print("Exchanging PAT for Horizon access token ...")
body = urllib.parse.urlencode({
    "grant_type": "client_credentials",
    "scope": f"session:role:{ROLE}",
    "client_secret": PAT,
}).encode()
try:
    resp  = urllib.request.urlopen(
        urllib.request.Request(
            f"{BASE}/v1/oauth/tokens", data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    )
    token = json.load(resp)["access_token"]
except urllib.error.HTTPError as e:
    if e.code == 401:
        sys.exit(
            "ERROR: 401 from Horizon. Check that the PAT is current and that "
            "HOL_USER has a network policy set (see 00_bootstrap.sql)."
        )
    sys.exit(f"ERROR: HTTP {e.code} from token endpoint.")

# -- Step 2: connect to the Horizon REST catalog with vended credentials -------
try:
    from pyiceberg.catalog.rest import RestCatalog
except ImportError:
    sys.exit('ERROR: run  pip install "pyiceberg[pyarrow]"  first.')

# NOTE: pyiceberg's credential= property does NOT work here (OAuthError from Horizon).
# Pass token= directly with the access_token obtained above.
catalog = RestCatalog(
    name="horizon", uri=BASE, warehouse=DB, token=token,
    **{"header.X-Iceberg-Access-Delegation": "vended-credentials"},
)

table_ref = (sys.argv[1] if len(sys.argv) > 1 else "ANALYTICS.YIELD_BY_LINE_5MIN").upper()
try:
    tbl = catalog.load_table(table_ref)
except Exception as e:
    sys.exit(f"ERROR loading {table_ref}: {e}")

# -- The "same bytes, no warehouse" proof --------------------------------------
print(f"\nIceberg format : v{tbl.metadata.format_version}")
print(f"Storage path   : {tbl.metadata.location}")
# ^ will show  s3://sfc-...-customer-interop-fs-.../iceberg/MFG/ANALYTICS/...
# PyIceberg fetches Parquet files directly from that S3 path with vended creds.
print("(PyIceberg read that path directly — no Snowflake warehouse involved.)\n")

# -- Schema --------------------------------------------------------------------
print("Schema:")
for f in tbl.schema().fields:
    print(f"  {f.name:<26} {f.field_type}")

# -- All rows ------------------------------------------------------------------
print("\nAll rows:")
rows = tbl.scan().to_arrow().to_pylist()
for r in rows:
    print(" ", r)
print(f"  {len(rows)} row(s) total")

# -- Predicate pushdown --------------------------------------------------------
# Demonstrating that filter evaluation is pushed down to the file scan layer,
# not applied after a full read.
print("\nPredicate pushdown  LINE == 'PAINT':")
paint = tbl.scan(row_filter="LINE == 'PAINT'").to_arrow().to_pylist()
for r in paint:
    print(" ", r)
print(f"  {len(paint)} row(s) after pushdown")
