# Build Real-Time Pipelines on Iceberg with AI Agents

Virtual Hands-On Lab · **27 August 2026, 10:00 AM PT**

In about 70 minutes you will build a real-time manufacturing pipeline on Snowflake. Change data
capture from an operational database lands in **Apache Iceberg** tables, **Dynamic Tables** refine it
continuously, and an **AI agent** explains what is happening on the plant floor. You build it by
prompting **Cortex Code**, not by pasting SQL.

Every block labelled **Prompt** is something you paste into **Cortex Code** — not SQL, not a shell
command. Use the copy button in its top-right corner. Blocks labelled **Fast path** are finished SQL
for the same step, and running one is a legitimate way to do this lab, not a confession.

Use whichever fits where you are. If you are behind, run the file and catch up — you will still see
every checkpoint. If you are ahead, prompt for it and read what Cortex Code produces. Nobody should
be stuck watching a room move on without them.

## The scenario

**Cascade Cycleworks** makes bicycle frames. Three lines run in sequence —
**WELD → PAINT → ASSEMBLY** — and every frame is scanned at the end of each line as PASS or FAIL with
a defect code. That inspection data lives in the plant's MES on Postgres. Separately, sensors on each
station stream temperature, humidity, current and torque readings.

Right now the plant manager gets a yield report at the end of shift. By then a bad run has already
eaten a shift of material. She wants yield and scrap per line within a couple of minutes, so she can
walk over and stop it.

Two minutes is the target for a reason: a human has to walk to the paint booth. Sub-second precision
is wasted on that loop; end-of-shift batch is far too slow.

**The payoff is Part 5.** You will trigger a two-phase incident — booth humidity drifts up, then paint
defects spike — and ask the agent *why* yield dropped. To answer, it has to read across both data
sources and notice that the cause preceded the effect. Then you will watch inspectors overturn failed
frames and see yield **go back up**, including for time buckets that already reported.

## What you will understand by the end

Not a feature tour — these are the things that change how you build:

- Why `ICEBERG_VERSION_DEFAULT` resolves from your **session's** current schema while
  `EXTERNAL_VOLUME` and `CATALOG` resolve from the target schema, and why that forces a `USE SCHEMA`
  before every Iceberg `CREATE`.
- Why a real CDC connector writes a **journal** and applies it on a gate, rather than writing your
  destination table directly — and where the latency in that design actually lives.
- What a soft delete costs you downstream, and why one missing predicate silently corrupts a yield
  metric for ever.
- Why an aggregate that can go **down** as well as up is the hard case, and how Dynamic Tables handle
  a correction that rewrites history.
- Why joining a second source is what lets an agent answer *why* instead of only *what*.
- How a bundled Cortex Code skill turns a one-line prompt into exactly the right object — the most
  transferable thing here.

## The architecture

![Two feeds land in Snowflake-managed Apache Iceberg v3 tables. A simulated Openflow Postgres CDC connector appends change events to a journal table over Snowpipe Streaming, and an append-only stream feeds a MERGE that maintains the QUALITY_INSPECTIONS destination table with soft deletes. In parallel, station telemetry streams directly into the STATION_TELEMETRY Iceberg table. Four Dynamic Iceberg Tables refine both feeds incrementally: INSPECTIONS_ACTIVE filters soft-deleted rows and STATION_HEALTH rolls up telemetry, then YIELD_BY_LINE_5MIN joins scans to telemetry per five-minute bucket and DEFECT_COUNTS_5MIN counts defects. A semantic view sits over the gold tables, a Cortex Agent answers questions over the semantic view, and PyIceberg reads the same gold tables from outside Snowflake through the Horizon Catalog.](docs/architecture.svg)

## What you need

Everything here is **pre-work**. Nothing installs during the session.

- A Snowflake trial account, created with the signup link provided for this event. You will be
  ACCOUNTADMIN.
- **Cortex Code Desktop** — the desktop application specifically
  ([download](https://www.snowflake.com/en/product/snowflake-coco/downloads/)). Not the CLI: the
  Cortex Code **CLI** is unavailable on standard Snowflake trial accounts. Not the Snowsight version
  either: it has no local shell or filesystem, so it cannot create a virtual environment or run the
  producer.
- **Snowsight** in a browser tab, logged in to the same account. You will switch to it twice.
- **Git** and **Python 3.9+** locally.
- About 70 minutes for the lab, plus 15 minutes of setup you do beforehand.

## Repo layout

```
producer/                the data producer. Start it once, in Part 2, and leave it running.
  main.py                the runner -- this is the file you invoke
  cdc_simulator.py       the simulated Openflow connector: creates its own objects,
                         writes the journal, issues the MERGE. Worth reading.
  telemetry.py           the station sensor feed, straight into Iceberg
  control.py             polls SIMULATOR_CONTROL, so Part 5 needs no restart
  common.py              object names, logging, credentials
  requirements.txt       two pinned packages
  profile.example.json   the shape of the profile.json you build in Setup D
solutions/               the fast path -- finished SQL for every Part, safe to run any time
  00_bootstrap.sql       account settings + the HOL_USER login and its token (Setup B)
  01_environment.sql     database, both schemas + Iceberg defaults, warehouse, telemetry table,
                         control table. NOT the CDC objects -- the connector makes those.
  02_preflight.sql       four checks that must all be TRUE before you build anything on top
  03_journal_inspection.sql  look at what the connector built, and the merge gate
  04_dynamic_tables.sql  all four Dynamic Iceberg Tables
  05_semantic_view.sql   PLANT_FLOOR_SV, plus its three checkpoint queries
  06_agent.sql           the Cascade Plant Analyst agent
  progress.sql           "where am I" -- every object, built or not, with row counts
  09_cleanup.sql         stop the spend, then optionally remove everything
external/
  read_iceberg.py        read the Gold table from your laptop via PyIceberg. Optional act A.
docs/
  architecture.svg       the diagram at the top of this file
  agent_questions.md     the three agent questions, verbatim
dashboard/               the live dashboard the presenter shares on screen. Not a lab step.
.snowflake/              a Cortex Code skill that loads automatically (see the last section)
```

---

# Setup — do this before the session

Budget 15 minutes. If you arrive with all four checkpoints green, you will keep pace.

## A. Get the lab files

```bash
git clone https://github.com/sfc-gh-kgaputis/streaming-cdc-iceberg-vhol.git
cd streaming-cdc-iceberg-vhol
```

Open this folder in Cortex Code Desktop. The bundled skill loads automatically because it lives in
`.snowflake/cortex/skills/` — there is nothing to install.

## B. Bootstrap the account (Snowsight)

Log in to Snowsight as your signup ACCOUNTADMIN, open a worksheet, and run the SQL below. It is also
in [`solutions/00_bootstrap.sql`](solutions/00_bootstrap.sql).

This one block is SQL you paste rather than a prompt, for a specific reason: Cortex Code cannot create
the login it is about to connect as.

```sql
USE ROLE ACCOUNTADMIN;

-- The producer emits UTC. Without this, every latency measurement below is off
-- by your UTC offset.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- REQUIRED for the agent in Part 4. Defaults to DISABLED on a fresh account,
-- which shrinks the available models and Cortex features.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- One identity for both Cortex Code and the producer, so you manage one credential.
CREATE USER IF NOT EXISTS HOL_USER
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Iceberg CDC VHOL lab user';
GRANT ROLE ACCOUNTADMIN TO USER HOL_USER;

-- Cortex access is NOT implied by ACCOUNTADMIN. Without these the agent step fails.
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER  TO ROLE ACCOUNTADMIN;
GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER TO ROLE ACCOUNTADMIN;

-- A token is refused authentication unless its user sits under a network policy --
-- even though Snowflake will happily mint one without it.
CREATE NETWORK POLICY IF NOT EXISTS HOL_NP ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER HOL_USER SET NETWORK_POLICY = HOL_NP;

ALTER USER HOL_USER
  ADD PROGRAMMATIC ACCESS TOKEN HOL_PAT
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 7
    COMMENT = 'Iceberg CDC VHOL lab token';
```

**Copy the `token_secret` value now — it is shown once.** Create a file called `secret.pat` in the
root of this repo and paste the token into it. It is gitignored.

**Missed it?** The token really is unrecoverable, but you do not need to start over — rotate it to get
a fresh one, in Snowsight as your signup admin:

```sql
ALTER USER HOL_USER ROTATE PROGRAMMATIC ACCESS TOKEN HOL_PAT;
```

That returns a new `token_secret`. Copy *that* into `secret.pat`. Do not try to create the token by
prompting Cortex Code — it is not connected yet, which is the whole reason this one block is SQL you
paste rather than a prompt.

Then run this and copy the result:

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
```

**Checkpoint:** `SHOW USERS LIKE 'HOL_USER'` returns one row, and you have the token text saved in
`secret.pat` and the account identifier on your clipboard.

## C. Connect Cortex Code Desktop as HOL_USER

Add a connection using the `account_identifier` from above, user `HOL_USER`, and your token from
`secret.pat` as the credential. Role `ACCOUNTADMIN`.

Then confirm it:

**Prompt:**

```text
Test my Snowflake connection and confirm the lab skill is loaded.
```

**Checkpoint:** user comes back as `HOL_USER`, role `ACCOUNTADMIN`, region starts with `AWS_`, and
Cortex Code names the `coco-iceberg-cdc-vhol` skill as active.

## D. Set up the producer environment

**Prompt:**

```text
Set up the data producer environment: the venv, and producer/profile.json.
```

The skill handles the rest — detecting your OS, using the right interpreter path for it, and why macOS
needs a virtual environment. Two packages, a few seconds.

**Checkpoint:** `producer/profile.json` exists with five keys, and this prints rows without touching
Snowflake:

```bash
.venv/bin/python producer/main.py --dry-run --cdc --seed 42
```

Your token is never printed to the chat.

---

# Run the lab

Five Parts, **66 minutes**. Two optional acts follow the core — do them if you are ahead.

Each Part names how you should drive Cortex Code:

- **Sequential prompts** — one prompt, check, next prompt. Slowest and safest.
- **Generate then confirm** — let it propose the SQL, read it, then approve. Use this on DDL you care
  about.
- **Direct execution** — let it run without reviewing each statement. Fine for read-only inspection.

## Part 1 — Land both feeds in Iceberg · 12 min

**Approach: generate then confirm.** This Part creates every object the rest of the lab stands on, and
one of them has a failure mode that only surfaces four Parts later. Read the DDL before you approve
it.

This is a good place to use **Plan Mode** (`Shift+Tab` in Cortex Code) — it makes Cortex Code lay out
the whole sequence before it executes anything, so you can see the `USE SCHEMA` statements below in
context. This is the one Part where that is worth the extra step.

**Prompt:**

```text
Create the lab environment and both landing tables.
```

Two schemas, and the split is the shape of the pipeline rather than the shape of the feeds:

| Schema | Holds | Why |
|---|---|---|
| `MFG.RAW` | The CDC destination table, its journal and stream, and the telemetry table | Both feeds land here. The journal sits beside its destination table because that is where the real Openflow connector puts it. |
| `MFG.ANALYTICS` | All four Dynamic Tables, the semantic view, the agent | Everything derived. Nothing writes here — Snowflake maintains all of it. |

By the end you will be able to read the pipeline off a fully-qualified name. A table in `RAW` arrived
from outside; a table in `ANALYTICS` was computed for you.

Watch for a `USE SCHEMA` before each Iceberg `CREATE`. That is not cosmetic, and it is the least
obvious thing in this lab:

**`ICEBERG_VERSION_DEFAULT` resolves from your session's current schema, not from the schema holding
the table you are creating.** `EXTERNAL_VOLUME` and `CATALOG` resolve the way you would expect, from
the target schema. So a `CREATE ICEBERG TABLE MFG.RAW.T` issued while your session is somewhere else
gets the right storage and silently lands on Iceberg **v2** — while `SHOW PARAMETERS` keeps reporting
`3`. A v2 table then rejects `VARIANT` and rejects the `TIMESTAMP_NTZ(9)` that `TIME_SLICE()`
produces, far from the actual cause. `CREATE DYNAMIC ICEBERG TABLE` has no version clause at all, so
for the Dynamic Tables there is no way to override it per statement. And Iceberg has no in-place
v2 → v3 upgrade — a table that lands wrong has to be recreated.

Notice what the telemetry table's DDL does **not** contain: no `CATALOG`, no `EXTERNAL_VOLUME`, no
version. That is the point. It inherits.

**Checkpoint:** `SHOW ICEBERG TABLES IN DATABASE MFG` lists `STATION_TELEMETRY`, and
`SHOW TABLES LIKE 'QUALITY_INSPECTIONS' IN SCHEMA MFG.RAW` lists a table that is **not** Iceberg. That
asymmetry is deliberate — the CDC destination is a standard table because it is rewritten constantly.

You created **one** data table just now. The connector will create three more for itself when you
start it in Part 2 — the CDC destination, the change journal and its stream. That split is not
arbitrary, and it is worth thirty seconds:

| Ingestion path | Who creates the target table |
|---|---|
| **Openflow CDC connector** | The **connector**. It "creates the schemas and destination tables matching the source tables" — you point it at a source and the objects appear. |
| **Snowpipe Streaming client** | **You do.** The SDK auto-creates the *pipe*, never the table. Creating it is step 2 of Snowflake's own streaming quickstart. |

So a managed connector provisions its own destination; a streaming application does not. If you take
one operational fact home from Part 1, take that one — it decides who owns your schema.

It is also why `STATION_TELEMETRY` above carries **no** `CATALOG`, `EXTERNAL_VOLUME` or
`ICEBERG_VERSION` clause. It inherits all three, and the preflight is about to prove it did. The
connector's own DDL does the opposite and states every property explicitly, because it should be
immune to the trap rather than demonstrating it.

Now verify everything, before building anything on top:

**Prompt:**

```text
Run the preflight checks.
```

**Checkpoint:** `aws_ok`, `cortex_ok`, `raw_iceberg_ok` and `analytics_iceberg_ok` all come back TRUE, every
Iceberg object reports `is_v3 = TRUE`, and the stream reports `mode = APPEND_ONLY`.
**Do not continue past a FALSE** — see [Troubleshooting](#troubleshooting). There is no in-place
v2 → v3 upgrade, so a wrong answer here gets more expensive with every Part.

## Part 2 — Watch the connector's change feed · 12 min

**Approach: direct execution.** Everything here is read-only.

**Prompt:**

```text
Start the producer in the background with both sources, then verify rows are landing.
```

**Watch the first three lines it prints.** Before it streams anything, the connector creates its own
targets and says so:

```
[connector] destination table ready
[connector] journal ready
[connector] journal stream ready
```

That is the division of labour the lab is teaching. You built the environment and the telemetry table;
the connector builds the CDC objects. If those three lines are missing, it found them already there.

**You start this once and leave it running for the rest of the lab.** You will never stop it, and
you will never restart it. That is the point: a streaming pipeline is something you turn on and
operate, not something you cycle every time conditions change.

**Keep its output where you can see it.** It reports what the plant floor is doing, once a second:

```
[telem] rows=1860 booth_humidity~44.0
[merge] gate fired: 122 rows applied in 1.5s (merges=1 rows_total=122)
[cdc] inserts=62 updates=0 soft_deletes=1
```

That log is the fastest answer to "is it working?", and in Part 5 it is where you will see the
incident begin — several seconds before any query shows it.

Two sources doing two different jobs:

- **CDC** → the journal, over Snowpipe Streaming. Stands in for Openflow's Postgres CDC connector:
  inserts new scans, **updates** them when an inspector re-checks a frame, and **soft-deletes** voided
  duplicate scans.
- **Telemetry** → `STATION_TELEMETRY`, also Snowpipe Streaming, at ~60 rows/sec.

Only the *connector* is simulated. Everything downstream is exactly what you would build for real.

**In steady state:** telemetry climbs fastest by far — roughly 900 rows per 15 seconds against a
couple of scans per second — so expect telemetry in the tens of thousands while the journal is in the
low thousands. That ratio is correct, not a fault.

**Checkpoint:** journal events, destination rows and telemetry rows all climb when you re-run the
query. Telemetry lag is **~30 seconds** — that is `MAX_CLIENT_LAG`, which defaults to 30 s for Iceberg
targets so Snowflake can size Parquet files sensibly. Expected, not a fault.

### While that first 30 seconds passes

**Prompt:**

```text
Show me the pipes and channels for these tables.
```

**Checkpoint:** one pipe per target table, each with a name you did not choose, and no `CREATE PIPE`
anywhere in this lab or in `solutions/`. Snowpipe Streaming auto-created them.

Now the part that is actually about change data capture:

**Prompt:**

```text
Show me the journal's change events, the event-type mix, and the destination's lag.
```

| `EVENT_TYPE` | What it carries |
|---|---|
| `IncrementalInsertRows` | every `PAYLOAD__*` populated |
| `IncrementalUpdateRows` | `PAYLOAD__*` holds the **new** values; `PRIMARY_KEY__*` the **old** key |
| `IncrementalDeleteRows` | every `PAYLOAD__*` is **NULL** — the key alone identifies the row |

That last row is why the MERGE's insert branch needs
`IFF(EVENT_TYPE='IncrementalDeleteRows', PRIMARY_KEY__INSPECTION_ID, PAYLOAD__INSPECTION_ID)`.

**In steady state:** the gap is roughly one minute of change events — around a hundred rows at the
default rate. It grows until the gate fires, drops, and grows again. A gap that never shrinks means
the merge is not running; a gap of zero means you are looking between a merge and its next batch.

**Checkpoint:** the journal count **exceeds** the destination count. That gap is the merge gate, not a
backlog. Each merge starts at second **:00** of a minute and finishes in a second or two. That
contrast is the honest lesson about where the latency lives: it is a schedule you chose, not a
throughput limit.

You will never build a Dynamic Table on this journal. It is connector-internal, its schema shifts with
a generation counter, and the connector prunes it. You build on the destination table.

### Two deep-dives, while the pipeline settles

**Prompt:**

```text
Show me SF_METADATA, what type it really is, and pull the offset token out of it.
```

`SF_METADATA` is a `VARIANT` holding a JSON **string**, not a parsed object, because that is what the
connector writes.

**Checkpoint:** `SF_METADATA:offset_token` returns `NULL` and `TYPEOF(SF_METADATA)` says `VARCHAR`,
while `PARSE_JSON(SF_METADATA::STRING):offset_token` returns an actual offset. Both are true at once,
which is the whole lesson: a `VARIANT` column is not a promise that its contents are parsed.

**Prompt:**

```text
Find the connector's merges in query history using its query tag.
```

The connector stamps every merge with a `QUERY_TAG` identifying itself, its operation and its merge
strategy. Filtering `QUERY_HISTORY` on that tag is how you would audit a real Openflow deployment, and
it works identically here.

**Checkpoint:** roughly one MERGE per minute since you started the producer, each beginning at second
`:00` and finishing in a second or two. Many short merges on a schedule, not one long-running one.

## Part 3 — Refine it with Dynamic Tables · 14 min

**Approach: generate then confirm.** One predicate in here is the difference between a correct
pipeline and a plausible-looking wrong one. Read for it.

**Prompt:**

```text
Create the two layer-one Dynamic Tables, INSPECTIONS_ACTIVE and STATION_HEALTH.
```

`INSPECTIONS_ACTIVE` carries the predicate that matters: `WHERE NOT _SNOWFLAKE_DELETED`. Omit it and
voided frames count against yield forever.

**In steady state:** `INSPECTIONS_ACTIVE` tracks the destination table closely — a little smaller,
because soft-deleted rows are filtered out. `STATION_HEALTH` is tiny by comparison: it is one row per
station, metric and 5-minute bucket, so a few dozen rows is right even with tens of thousands of
readings underneath.

**Checkpoint:** `SHOW DYNAMIC TABLES` reports `refresh_mode = INCREMENTAL`, `is_iceberg = true`, and
an **empty** `refresh_mode_reason` for both. A populated `refresh_mode_reason` names its own cause —
read it rather than guessing.

**Prompt:**

```text
Create the two Gold Dynamic Tables, YIELD_BY_LINE_5MIN and DEFECT_COUNTS_5MIN.
```

`YIELD_BY_LINE_5MIN` is the join that earns the second data source: yield and booth humidity in the
same row, for the same 5-minute interval. Yield alone tells you PAINT is scrapping frames. Yield
beside humidity tells you *why*.

`AVG_BOOTH_HUMIDITY` is empty for WELD and ASSEMBLY. That is correct — booth humidity is a paint-booth
metric.

**In steady state:** each line sits around **96–100%** first-pass yield, with a handful of scrap units
per bucket. `AVG_BOOTH_HUMIDITY` reads about **44** for PAINT and is empty for WELD and ASSEMBLY.
Single-digit row counts are correct — three lines times the number of elapsed 5-minute buckets.

**Checkpoint:** all four Dynamic Tables now report `refresh_mode = INCREMENTAL`, and
`YIELD_BY_LINE_5MIN` holds three rows per elapsed 5-minute bucket — single digits early on. It is
much smaller than `QUALITY_INSPECTIONS` by design; that is what aggregation means.

**Prompt:**

```text
Show me the refresh history for these Dynamic Tables.
```

**Checkpoint:** the per-refresh row counts stay small even as the base table grows. Snowflake is
recomputing only the 5-minute groups that changed — while the source underneath is being UPDATEd and
DELETEd continuously by the connector's merges.

### Where am I?

At any point, in any Part:

**Prompt:**

```text
Show me the lab progress query.
```

**Checkpoint:** it lists every object you should have built by now with its row count, and flags
anything missing. This is also the fastest way for the presenter to see who is stuck.

## Part 4 — Ask it questions in English · 12 min

**Approach: sequential prompts.** Two objects, and the second depends on the first being right.

**Prompt:**

```text
Create the semantic view PLANT_FLOOR_SV, then run its three checkpoint queries.
```

**Checkpoint:** all three `SEMANTIC_VIEW()` queries return rows. In steady state each line sits around
95–99% first-pass yield.

**Prompt:**

```text
Create the Cascade Plant Analyst agent over PLANT_FLOOR_SV.
```

The agent's Analyst tool needs a warehouse named in `execution_environment`. Without it `CREATE AGENT`
succeeds and then every question fails with an opaque internal error that mentions neither the
warehouse nor the tool. The skill knows this; check the generated spec has it.

Now switch to **Snowsight → AI & ML → Agents → Cascade Plant Analyst** and use the chat panel on the
detail page. You do not need to Publish. Ask the first two questions from
[`docs/agent_questions.md`](docs/agent_questions.md):

1. *What is first-pass yield by line right now?*
2. *Which defect is driving scrap on PAINT?*

**Checkpoint:** the numbers match what the semantic view returned, and the agent names the 5-minute
interval it used. Keep this tab open — you need it in Part 5.

## Part 5 — The incident, and the recovery · 16 min

**Approach: direct execution**, then read.

The producer is still running from Part 2, and it stays running. What changes is the **plant**, not
the pipeline — you write a row to a control table and the running simulator picks it up within about
ten seconds:

**Prompt:**

```text
Set the simulator control mode to INCIDENT.
```

**Fast path:** if you would rather not wait for a prompt, this is all it does:

```sql
INSERT INTO MFG.RAW.SIMULATOR_CONTROL (MODE, UPDATED_AT)
  VALUES ('INCIDENT', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
```

**Checkpoint:** the producer's log shows `booth_humidity` climbing away from 44 within seconds —
`47.7`, `52.2`, `56.7` — and about 90 seconds later `[cdc] PAINT defect rate -> 26%`. You are
watching cause precede effect in real time, before a single query.

This is worth pausing on. A real Openflow connector runs continuously; when a paint booth starts
misbehaving nobody restarts the connector. The data changes character at the source and the pipeline
carries it through untouched. The control table is the source changing its mind — the streaming
plumbing never notices.

Now watch the cascade arrive layer by layer, and time it:

| What | Where it shows up | When |
|---|---|---|
| Booth humidity climbs ~44 → ~70 | `STATION_HEALTH` | ~30–60 s |
| PAINT defects spike, `PAINT_RUN` dominates | `DEFECT_COUNTS_5MIN` | ~90 s later |
| PAINT yield falls into the **80s** | `YIELD_BY_LINE_5MIN` | ~1–2 min after that |

WELD and ASSEMBLY stay in the high 90s throughout — they are your control.

Now the payoff. In the Snowsight agent tab, ask question 3:

3. *Why did PAINT yield drop?*

**Checkpoint:** the agent connects the humidity rise to the `PAINT_RUN` defects and gets the **order**
right — humidity first, defects second. That answer is only possible because two sources were joined
in Part 3. An agent on the CDC feed alone could tell you *what* happened and never *why*.

Then the recovery:

**Prompt:**

```text
Set the simulator control mode to REINSPECT.
```

**Fast path:**

```sql
INSERT INTO MFG.RAW.SIMULATOR_CONTROL (MODE, UPDATED_AT)
  VALUES ('REINSPECT', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
```

Inspectors re-check failed frames and overturn them to PASS. This is an `UPDATE` arriving over CDC,
flowing through the journal and the MERGE, and it **rewrites history** — buckets that already reported
now report better numbers.

**Checkpoint:** PAINT yield climbs back, including for earlier buckets, and the Dynamic Tables are
still `INCREMENTAL`. An append-only pipeline cannot do this; it would have double-counted the frame or
ignored the correction entirely.

---

# Optional acts

Core lab done. Both of these stand alone — do either, both, or neither.

## Optional A — Read your Iceberg tables from your laptop · 5 min

The claim this lab makes is that your data is in **open** Iceberg, governed by Snowflake but not
locked inside it. This proves it. PyIceberg reads the Gold Dynamic Table straight from object storage
through the Horizon Catalog, using vended credentials — no Snowflake warehouse involved in the read.

```bash
pip install -r external/requirements.txt
python external/read_iceberg.py
```

This one ships pre-written rather than prompted, deliberately. The auth path has two non-obvious steps
that are not in the PyIceberg docs, and a broken first draft would cost more than it teaches. Read the
script — it is 100 lines and the comments explain both traps.

**Checkpoint:** it prints Iceberg format version `v3`, a storage path under Snowflake's managed bucket
(`s3://sfc-…-customer-interop-fs-…`), your rows, and a smaller row count after predicate pushdown on
`LINE == 'PAINT'`.

That storage path is the whole point: those are the same bytes Snowflake reads, read by an engine that
has never heard of Snowflake.

## Optional B — Break it on purpose · 3 min

**Prompt:**

```text
Add a top-defect column to DEFECT_COUNTS_5MIN using MODE(DEFECT_CODE).
```

**Checkpoint:** it fails at `CREATE` time, not at refresh time:
*"Change tracking is not supported on queries containing the function 'MODE'"*. That is why defects are
counted at their natural grain and ranked at read time instead — a constraint that shaped the design,
found the only way anyone finds these.

---


# Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `raw_iceberg_ok` or `analytics_iceberg_ok` is FALSE, or an object reports v2 | Your session's current schema was not one that resolves `ICEBERG_VERSION_DEFAULT = 3` when the table was created | Re-run `01_environment.sql` — it sets the database-level default and issues `USE SCHEMA` before each create. Then **recreate** any v2 table: Iceberg has no in-place v2 → v3 upgrade. |
| `Unsupported data type 'VARIANT' for iceberg tables` | Same cause — the table resolved to v2 | Same fix. This is the error the journal throws, since `SF_METADATA` is `VARIANT`. |
| `SHOW PARAMETERS` says 3 but tables come out v2 | Not a contradiction. That parameter is reported per schema but applied per *session* schema | `USE SCHEMA MFG.RAW;` immediately before the `CREATE`. Never trust `SHOW PARAMETERS` as proof. |
| `cortex_ok` is FALSE | `CORTEX_ENABLED_CROSS_REGION` still `DISABLED` | Re-run that `ALTER ACCOUNT` from Setup B as ACCOUNTADMIN in Snowsight. |
| Rejected `TIMESTAMP_NTZ(9)` from `TIME_SLICE()` | The Dynamic Table landed on v2, and it has no version clause to override | Fix the session-schema issue above, then recreate the Dynamic Table. |
| `refresh_mode` comes back `FULL` | Something in the query blocks incremental refresh | Read `refresh_mode_reason`; it names the cause. `APPROX_PERCENTILE` is a common one. |
| `Change tracking is not supported ... 'MODE'` | `MODE()` in a Dynamic Table | Expected — that is Optional B. Count at defect grain, rank at read time. |
| Destination table stays behind the journal | That is the merge gate, by design | Check `QUERY_HISTORY` for the connector's `QUERY_TAG`. Merges fire at second :00 each minute. Lower `--merge-gate-seconds` to shrink the gap. |
| Destination table gets **no** rows at all | The producer was started with `--no-merge`, or the journal objects do not exist | Restart the producer without `--no-merge`, and confirm the journal and its stream exist. |
| `SF_METADATA:offset_token` returns NULL | It holds a JSON string, not an object — faithful connector behaviour | `PARSE_JSON(SF_METADATA::STRING):offset_token` |
| Telemetry rows take ~30 s to appear | `MAX_CLIENT_LAG` defaults to 30 s for Iceberg | Expected behaviour, not a fault. |
| Producer: `ERR_CHANNEL_HAS_UNCOMMITTED_DATA` (HTTP 409) | You stopped the producer and started it again within ~30 s, reopening a channel that was still committing | The lab never asks you to restart it — Part 5 changes modes through the control table instead. If you did stop it, wait ~30 s. Never run two producers at once. |
| Producer: `externally-managed-environment` | macOS Homebrew Python, PEP 668 | Use the venv interpreter, not system Python. Ask Cortex Code to redo the venv step. |
| Snowsight: `SQL compilation error: Empty SQL statement` at the end of a `solutions/` file | Snowsight parses whatever follows the last statement as a statement, so a file ending in comments errors | Harmless — everything above it ran. Every file now ends with a `SELECT '… complete'` so you get a confirmation row instead. If you see it, check the statements above succeeded. |
| You did not copy `token_secret` in time | It is shown exactly once | `ALTER USER HOL_USER ROTATE PROGRAMMATIC ACCESS TOKEN HOL_PAT;` in Snowsight returns a fresh one. Do not prompt Cortex Code for it — it is not connected yet. |
| Part 5: `SIMULATOR_CONTROL does not exist`, or the producer logs `[control] read failed` | Part 1 created the landing tables but not the control table | Run [`solutions/01_environment.sql`](solutions/01_environment.sql), which creates all three. Then re-run the Part 5 prompt. |
| Producer: authentication fails | Token expired, or `profile.json` has the wrong account | Tokens last 7 days. Re-mint in Snowsight and rebuild `profile.json`. |
| Agent: `internal error (request_id: …)`, code 391920 | The Analyst tool has no `execution_environment`, so its generated SQL has no warehouse to run in | Add `"execution_environment": { "type": "warehouse", "warehouse": "HOL_WH" }` to the `tool_resources` entry and re-run `CREATE OR REPLACE AGENT`. |
| Agent answers with stale numbers | The pipeline lags 1–2 min by design | Ask again in a minute. "Right now" means the most recent complete buckets. |
| Agent errors or lists no models | Cross-region inference disabled | See `cortex_ok` above. |
| Agent: *"not an allowed model for Agent"* | A specific orchestration model was pinned | Use `"orchestration": "auto"`. Agent orchestration has a narrower allowed-models list than Cortex `COMPLETE`. |
| External read: 401 with an empty body | A PAT presented directly as a Bearer token. It must be exchanged for an access token first | `external/read_iceberg.py` does the exchange. If you wrote your own, see the comments in it. |
| External read: `OAuthError: unauthorized_client` | PyIceberg's `credential` property formats the request in a way Horizon rejects | Pass `token=<access_token>` instead. |
| External read: HTTP 404 on the catalog | Catalog or namespace name is lower-case | Uppercase them. `warehouse=` is the **database** name, uppercase. |
| Wrong account shows up in `profile.json` | The `cortex` CLI's default connection was used instead of the active one | The account must come from SQL: `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME()`. |

---

# The producer (reference)

You never need to edit it. Run it with the venv interpreter so it finds the SDK —
`.venv/bin/python` on macOS/Linux, `.venv\Scripts\python.exe` on Windows.

You start it **once**, in Part 2, with both sources:

```bash
.venv/bin/python producer/main.py --profile producer/profile.json --cdc --telemetry
```

That is the only command the lab asks you to run. The incident and the recovery are triggered by
writing to `MFG.RAW.SIMULATOR_CONTROL` while it keeps streaming — see Part 5.

```bash
# see what it generates, no Snowflake account needed
.venv/bin/python producer/main.py --dry-run --cdc --seed 42
```

`--incident` and `--reinspect` also exist as startup flags, and `--no-control` ignores the control
table entirely. Those are for someone rehearsing the lab from a shell; you do not need them, and
using them means stopping the producer, which is the one thing this design avoids.

`--rate` sets scans/sec (default 2), `--telemetry-rate` sets telemetry rows/sec (default 60).
`--seed` makes a run reproducible. `--help` lists the rest.

`--cdc-mode` picks how the CDC half writes:

- `journal` (default) — the faithful path. Change events go to the journal over Snowpipe Streaming,
  and the producer issues the MERGE on its CRON gate. This is what the lab teaches.
- `direct` — writes the settled result straight to `QUALITY_INSPECTIONS` with ordinary DML. No journal,
  no stream, no merge gate. Use it only if the journal objects are missing and you need rows flowing
  to catch up; it loses the merge gate and the two-path design.

# How the skill knows all this

`.snowflake/cortex/skills/coco-iceberg-cdc-vhol/` holds a Cortex Code **skill**: the object model, the
measured Iceberg constraints, the checkpoint queries, and verbatim DDL for the trickier objects.
Cortex Code loads it automatically when you open this folder, which is why a one-line prompt produces
exactly the right table.

Open `SKILL.md` and read it. Writing one for your own stack is the most transferable thing you will
take away from this lab — it is how you stop re-explaining your conventions to an agent on every task.

# Cleanup

**Do not skip this.** The Dynamic Tables refresh every minute for as long as they exist, and will
quietly consume trial credits for days.

**Prompt:**

```text
Run the cleanup script.
```

**Checkpoint:** `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS` reports `scheduling_state = SUSPENDED` for all
four, or returns nothing at all if you removed them.

Or run [`solutions/09_cleanup.sql`](solutions/09_cleanup.sql) yourself. Block 1 stops the spend and
keeps your data; Block 2 removes everything. Then stop the producer and delete your local `secret.pat`
and `producer/profile.json`.

# License

Apache 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

# Disclaimer

Sample code and content for educational purposes, provided as-is without warranty. Not intended for
production use. Cascade Cycleworks, the scenario, and all data in this lab are fictional.
