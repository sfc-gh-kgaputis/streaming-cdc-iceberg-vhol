# Build Real-Time Pipelines on Iceberg with AI Agents

Virtual Hands-On Lab · **27 August 2026, 10:00 AM PT**

You will build a real-time manufacturing pipeline on Snowflake. Change data capture
from an operational database lands in **Apache Iceberg** tables, **Dynamic Tables**
refine it continuously, and an **AI agent** explains what is happening on the plant
floor. You build it by prompting **Cortex Code**, not by pasting SQL.

## The scenario

**Cascade Cycleworks** makes bicycle frames. Three lines run in sequence —
**WELD → PAINT → ASSEMBLY** — and every frame is scanned at the end of each line as
PASS or FAIL with a defect code. That inspection data lives in the plant's MES on
Postgres. Separately, sensors on each station stream temperature, humidity, current
and torque readings.

Right now the plant manager gets a yield report at the end of shift. By then a bad
run has already eaten a shift of material. She wants yield and scrap per line within
a couple of minutes, so she can walk over and stop it.

Two minutes is the target for a reason: a human has to walk to the paint booth.
Sub-second precision is wasted on that loop; end-of-shift batch is far too slow.

**The payoff** is Part 7. You will trigger a two-phase incident — booth humidity
drifts up, then paint defects spike — and ask the agent *why* yield dropped. To
answer, it has to read across both data sources and notice that the cause preceded
the effect. Then you will watch inspectors overturn failed frames and see yield
**go back up**, including for time buckets that already reported.

## What you need

- A Snowflake trial account, created with the signup link provided for this event.
  You will be ACCOUNTADMIN.
- **Cortex Code** installed
  ([download](https://www.snowflake.com/en/product/snowflake-coco/downloads/)).
- Git and Python 3.9+ locally.
- About 90 minutes.

## Repo layout

```
producer/       the data producer (CDC simulator + Snowpipe Streaming) and its deps
solutions/      answer key, one file per part -- read these any time
.snowflake/     a Cortex Code skill that loads automatically (see the last section)
```

---

# Setup

## A. Get the lab files

```bash
git clone https://github.com/sfc-gh-kgaputis/streaming-cdc-iceberg-vhol.git
cd streaming-cdc-iceberg-vhol
```

Open this folder in Cortex Code. The bundled skill loads automatically because it
lives in `.snowflake/cortex/skills/` — there is nothing to install.

## B. Bootstrap the account (Snowsight)

Log in to Snowsight as your signup ACCOUNTADMIN, open a worksheet, and run this.
It is also in [`solutions/00_bootstrap.sql`](solutions/00_bootstrap.sql).

Cortex Code cannot create its own login, so this part has to happen first.

```sql
USE ROLE ACCOUNTADMIN;

-- The producer emits UTC. Without this, every latency measurement below is off
-- by your UTC offset.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- REQUIRED for the agent in Part 6. Defaults to DISABLED on a fresh account,
-- which shrinks the available models and Cortex features.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- One identity for both Cortex Code and the producer, so you manage one credential.
CREATE USER IF NOT EXISTS VHOLuser
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Iceberg CDC VHOL lab user';
GRANT ROLE ACCOUNTADMIN TO USER VHOLuser;

CREATE NETWORK POLICY IF NOT EXISTS vhol_np ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER VHOLuser SET NETWORK_POLICY = vhol_np;

ALTER USER VHOLuser
  ADD PROGRAMMATIC ACCESS TOKEN vhol_pat
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 7
    COMMENT = 'Iceberg CDC VHOL lab token';
```

**Copy the `token_secret` value now — it is shown once.** Create a file called
`secret.pat` in the root of this repo and paste the token into it. It is gitignored.

Then run this and copy the result:

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
```

## C. Connect Cortex Code as VHOLuser

Add a connection using the `account_identifier` from above, user `VHOLuser`, and
your token from `secret.pat` as the credential. Role `ACCOUNTADMIN`.

Then confirm it, and that the skill loaded:

> Test my Snowflake connection and confirm the lab skill is loaded.

**Checkpoint:** user comes back as `VHOLUSER`, role `ACCOUNTADMIN`, and Cortex Code
confirms the `coco-iceberg-cdc-vhol` skill is active. Region starts with `AWS_`.

## D. Set up the producer environment

> Set up the data producer environment: create the .venv and install its dependencies.

The skill handles the rest — detecting your OS, using the right interpreter path for
it, and why macOS needs a virtual environment. Two packages, a few seconds.

Then:

> Build producer/profile.json from my connection details and the token in secret.pat.

**Checkpoint:** `producer/profile.json` exists and has five keys. Your token is never
printed to the chat.

---

# Run the lab

## Part 1 — Environment and Iceberg tables · 10 min

> Create the lab environment: database MFG with schemas CDC and RAW, set the Iceberg
> defaults on both schemas, create the Gen2 XSMALL warehouse HOL_WH, then create the
> CDC destination table and the streaming telemetry table.

Watch for three `ALTER SCHEMA` statements setting `EXTERNAL_VOLUME`, `CATALOG` and
`ICEBERG_VERSION_DEFAULT = 3`. They are the most load-bearing statements in the lab:
`CREATE DYNAMIC ICEBERG TABLE` has no version clause, so it can only *inherit* them.
Skip them and tables either fail or silently land on Iceberg v2, which throws
confusing timestamp errors much later.

Notice also what the telemetry table's DDL does **not** contain: no `CATALOG`, no
`EXTERNAL_VOLUME`, no version. That is the point — it inherits.

Now verify, before building anything on top:

> Run the preflight checks.

**Checkpoint:** `aws_ok`, `cortex_ok` and `iceberg_ok` all come back TRUE, and the
Iceberg table reports `SNOWFLAKE_MANAGED` / `MANAGED` / `3`. **Do not continue past a
FALSE** — see Troubleshooting.

## Part 2 — Start both data sources · 8 min

> Start the producer in the background with both the CDC and telemetry sources, then
> verify rows are landing in both tables.

Two sources doing two different jobs:

- **CDC** → `PRODUCTION_SCANS`. Stands in for Openflow's Postgres CDC connector.
  Inserts new scans, **updates** them when an inspector re-checks a frame, and
  **soft-deletes** voided duplicate scans.
- **Telemetry** → `STATION_TELEMETRY`. Real **Snowpipe Streaming** into Iceberg, at
  ~60 rows/sec.

Only the *connector* is simulated. Everything downstream is exactly what you would
build for real.

**Checkpoint:** both counts climb when you re-run the query. CDC lag is a second or
two. Telemetry lag is **~30 seconds** — that is `MAX_CLIENT_LAG`, which defaults to
30 s for Iceberg targets so Snowflake can size Parquet files sensibly. Expected, not
a fault.

Worth a look while you are here:

> Show me the pipes and channels for the telemetry table.

There is no `CREATE PIPE` anywhere in this lab. Snowpipe Streaming auto-created a
default pipe for the table.

## Part 3 — Refine it: the soft-delete filter · 12 min

> Create the two layer-one Dynamic Iceberg Tables: DT_SCANS_ACTIVE and DT_STATION_HEALTH.

`DT_SCANS_ACTIVE` carries one predicate that matters more than it looks:
`WHERE NOT _SNOWFLAKE_DELETED`. The Openflow connector never hard-deletes — a voided
scan is still physically present, flagged. Omit that filter and voided frames count
against yield forever. This single predicate is the difference between a correct CDC
pipeline and a plausible-looking wrong one.

**Checkpoint:** `SHOW DYNAMIC TABLES` reports `refresh_mode = INCREMENTAL`,
`is_iceberg = true`, and an **empty** `refresh_mode_reason` for both.

## Part 4 — Gold, and proof of incremental refresh · 14 min

> Create the two Gold Dynamic Iceberg Tables: DT_YIELD_BY_LINE_5MIN joining the scans
> to the telemetry, and DT_DEFECT_COUNTS_5MIN.

`DT_YIELD_BY_LINE_5MIN` is the join that earns the second data source: yield and
booth humidity in the same row, for the same 5-minute interval. Yield alone tells you
PAINT is scrapping frames. Yield beside humidity tells you *why*.

`AVG_BOOTH_HUMIDITY` is empty for WELD and ASSEMBLY. That is correct — booth humidity
is a paint-booth metric.

> Show me the refresh history for these Dynamic Tables.

**Checkpoint:** the per-refresh row counts stay small even as the base table grows.
Snowflake is recomputing only the 5-minute groups that changed — while the source
underneath is being UPDATEd and DELETEd continuously.

Optional, and genuinely instructive:

> Try adding a top-defect column using MODE(DEFECT_CODE) and show me what happens.

It fails at `CREATE` time, not at refresh time:
*"Change tracking is not supported on queries containing the function 'MODE'"*. That
is why defects are counted at their natural grain and ranked at read time instead.

## Part 5 — A semantic view · 10 min

> Create the semantic view over the Gold tables, then run the three checkpoint queries.

**Checkpoint:** all three queries return rows. In steady state each line sits around
95–99% first-pass yield.

## Part 6 — The plant analyst agent · 12 min

> Create the Cascade Plant Analyst agent over the semantic view.

Then open **Snowsight → AI & ML → Agents → Cascade Plant Analyst** and use the chat
panel on the detail page. You do not need to Publish. Ask:

1. *What is first-pass yield by line right now?*
2. *Which defect is driving scrap on PAINT?*

**Checkpoint:** the numbers match what you saw in Part 5, and the agent tells you
which 5-minute interval it used.

Keep this tab open.

## Part 7 — The incident, and the recovery · 18 min

Restart the producer with the incident armed:

> Stop the producer and restart it in the background with the incident flag.

Now watch the cascade arrive layer by layer, and time it:

| What | Where it shows up | When |
|---|---|---|
| Booth humidity climbs ~44 → ~70 | `DT_STATION_HEALTH` | ~30–60 s |
| PAINT defects spike, `PAINT_RUN` dominates | `DT_DEFECT_COUNTS_5MIN` | ~90 s later |
| PAINT yield falls to the mid-70s | `DT_YIELD_BY_LINE_5MIN` | ~1–2 min after that |

WELD and ASSEMBLY stay in the high 90s throughout — they are your control.

Now the payoff. Ask the agent:

3. *Why did PAINT yield drop?*

**Checkpoint:** the agent connects the humidity rise to the `PAINT_RUN` defects and
gets the **order** right — humidity first, defects second. That answer is only
possible because two sources were joined in Part 4. An agent on the CDC feed alone
could tell you *what* happened and never *why*.

Then the recovery:

> Stop the producer and restart it in the background with the reinspect flag.

Inspectors re-check failed frames and overturn them to PASS. This is an `UPDATE`
arriving over CDC, and it **rewrites history** — buckets that already reported now
report better numbers.

**Checkpoint:** PAINT yield climbs back, including for earlier buckets, and the
Dynamic Tables are still `INCREMENTAL`. An append-only pipeline cannot do this; it
would have double-counted the frame or ignored the correction entirely.

---

# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `iceberg_ok` is FALSE in preflight | One of the three `ALTER SCHEMA` statements did not run | Re-run all three on both `MFG.CDC` and `MFG.RAW`, then re-run the preflight. Any Iceberg table created before them must be recreated. |
| `cortex_ok` is FALSE | `CORTEX_ENABLED_CROSS_REGION` still `DISABLED` | Re-run that `ALTER ACCOUNT` from Setup B as ACCOUNTADMIN in Snowsight. |
| `Unsupported data type 'VARIANT'` / odd timestamp scale errors | The table silently landed on Iceberg v2 | Same as above — the version default was missing. |
| `refresh_mode` comes back `FULL` | Something in the query blocks incremental refresh | Read `refresh_mode_reason`; it names the cause. `APPROX_PERCENTILE` is a common one. |
| `Change tracking is not supported ... 'MODE'` | `MODE()` in a Dynamic Table | Expected. Count at defect grain, rank at read time. |
| Telemetry rows take ~30 s to appear | `MAX_CLIENT_LAG` defaults to 30 s for Iceberg | Expected behaviour, not a fault. |
| Producer: `externally-managed-environment` | macOS Homebrew Python, PEP 668 | Use the venv interpreter, not system Python. Ask Cortex Code to redo the venv step. |
| Producer: authentication fails | Token expired, or `profile.json` has the wrong account | Tokens last 7 days. Re-mint in Snowsight and rebuild `profile.json`. |
| Agent answers with stale numbers | The pipeline lags 1–2 min by design | Ask again in a minute. "Right now" means the most recent complete buckets. |
| Agent errors or lists no models | Cross-region inference disabled | See `cortex_ok` above. |
| Wrong account shows up in `profile.json` | The `cortex` CLI's default connection was used instead of the active one | The account must come from SQL: `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME()`. |

---

# The producer (reference)

You never need to edit it. Run it with the venv interpreter so it finds the SDK —
`.venv/bin/python` on macOS/Linux, `.venv\Scripts\python.exe` on Windows.

```bash
# steady state, both sources
.venv/bin/python producer/producer.py --profile producer/profile.json --cdc --telemetry

# the incident: humidity drifts, then PAINT defects spike ~90s later
.venv/bin/python producer/producer.py --profile producer/profile.json --cdc --telemetry --incident

# the recovery: inspectors overturn failed frames, yield goes back up
.venv/bin/python producer/producer.py --profile producer/profile.json --cdc --telemetry --reinspect

# see what it generates, no Snowflake account needed
.venv/bin/python producer/producer.py --dry-run --cdc
```

`--rate` sets scans/sec (default 2), `--telemetry-rate` sets telemetry rows/sec
(default 60). `--help` lists the rest.

# How the skill knows all this

`.snowflake/cortex/skills/coco-iceberg-cdc-vhol/` holds a Cortex Code **skill**: the
object model, the measured Iceberg constraints, the checkpoint queries, and verbatim
DDL for the trickier objects. Cortex Code loads it automatically when you open this
folder, which is why a one-line prompt produces exactly the right table.

Open `SKILL.md` and read it. Writing one for your own stack is the most transferable
thing you will take away from this lab — it is how you stop re-explaining your
conventions to an agent on every task.

# Cleanup

**Do not skip this.** The Dynamic Tables refresh every minute for as long as they
exist, and will quietly consume trial credits for days.

> Run the cleanup script.

Or run [`solutions/09_cleanup.sql`](solutions/09_cleanup.sql) yourself. Block 1 stops
the spend and keeps your data; Block 2 removes everything. Then stop the producer and
delete your local `secret.pat` and `producer/profile.json`.

# License

Apache 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

# Disclaimer

Sample code and content for educational purposes, provided as-is without warranty.
Not intended for production use. Cascade Cycleworks, the scenario, and all data in
this lab are fictional.
