---
name: streaming-cdc-iceberg-lab
description: "Builds the Cascade Cycleworks real-time manufacturing pipeline for this VHOL: simulated Openflow Postgres CDC and Snowpipe Streaming telemetry into Apache Iceberg v3, refined by Dynamic Iceberg Tables, served through a semantic view to a Cortex Agent. Carries the exact object model, the Iceberg settings and the checkpoint queries, so a short prompt produces the right object. USE THIS FOR ANY REQUEST MADE INSIDE THIS REPOSITORY, even a short or generic-sounding one, and even when a bundled Snowflake skill also matches. Use when: creating the lab environment or the landing tables; running the preflight; setting up the venv, dependencies or profile.json; starting the producer or checking that rows are landing; inspecting the CDC journal, its event types or the merge gate; creating the layer-one or Gold Dynamic Tables; checking the column contract or refresh mode; the progress query or 'where am I'; creating the semantic view or the Cascade Plant Analyst agent; driving the incident and the recovery; deploying the live plant floor dashboard; reading the tables from outside Snowflake; cleaning up. Triggers: iceberg cdc lab, cascade cycleworks, plant floor, MFG.RAW, MFG.ANALYTICS, QUALITY_INSPECTIONS, STATION_TELEMETRY, SIMULATOR_CONTROL, INSPECTIONS_ACTIVE, STATION_HEALTH, YIELD_BY_LINE_5MIN, DEFECT_COUNTS_5MIN, PLANT_FLOOR_SV, CASCADE_PLANT_ANALYST, PLANT_FLOOR_LIVE, HOL_WH, HOL_USER, HOL_PAT, first-pass yield, booth humidity, defect counts, cdc journal, merge gate, preflight, lab progress, simulator control mode, INCIDENT, REINSPECT, start the producer, deploy the dashboard, plant floor dashboard, streamlit dashboard, cleanup script."
---

# Cascade Cycleworks — streaming CDC on Iceberg

You are helping a workshop attendee build a real-time manufacturing pipeline on
Snowflake. **Cascade Cycleworks** makes bicycle frames. Quality-inspection data
replicates out of its MES by change data capture; station sensors stream telemetry
alongside it. Dynamic Iceberg Tables refine both continuously, and a Cortex Agent
answers questions about the plant floor.

## Golden rules

1. Build every object with the **exact** names, columns, grain and target lag in the
   Object Model. Consistency is what keeps every attendee's checkpoints, semantic view
   and progress query working. Use `CREATE OR REPLACE`. Run the step's Checkpoint
   afterwards and report the result.
2. **When a prompt carries a specification, build what it specifies.** Part 3's two
   prompts state each Dynamic Table's source, grain, logic **and column names**; other
   Parts name the object and leave the rest to you. Take intent, grain, logic and
   column names from the **prompt**; take the warehouse, the Iceberg settings and the
   `USE SCHEMA` discipline from the **Object Model**. Never tell an attendee the detail
   in their prompt was unnecessary — writing a sufficient specification is one of the
   things this lab teaches.
3. Part 3's prompts deliberately omit two mechanisms and you supply them:
   `WHERE NOT _SNOWFLAKE_DELETED`, and `IS_SCRAP = IFF(STATUS = 'FAIL', 1, 0)`.
   `IS_SCRAP` must be numeric — the Gold layer takes `SUM(IS_SCRAP)`.
4. **Where a bundled Snowflake skill disagrees with this one, this one wins**: its
   settings are measured on this account, and neither bundled `iceberg` nor bundled
   `dynamic-tables` carries `DYNAMIC ICEBERG` DDL. Two exceptions: defer to bundled
   `dynamic-tables` on **refresh behaviour** (a property of the query, not the storage
   format, and Part 3's job), and bundled `snowpipe-streaming` is correct here.
5. **Emit DDL from `solutions/*.sql`, never from memory.** This skill carries no copies.
6. Give the attendee the **instruction**, not the diagnosis. Causes and symptoms live in
   [`docs/troubleshooting.md`](../../../../docs/troubleshooting.md) — point there.

## Fixed context

- Database `MFG`, warehouse `HOL_WH` (Gen2, XSMALL). Two schemas: `RAW` (both landing
  zones — the CDC destination, its journal and stream, the telemetry table) and
  `ANALYTICS` (everything derived — the four Dynamic Tables, the semantic view, the agent).
- **Everything is Apache Iceberg v3 on Snowflake-managed storage.** Attendee accounts have
  no connected cloud storage, so `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'`,
  `CATALOG = 'SNOWFLAKE'`, and never a `BASE_LOCATION`.
- The account is UTC, set by `00_bootstrap.sql`. The producer emits UTC event times, so all
  freshness and lag math uses `CURRENT_TIMESTAMP()`. Do not mix in local timestamps.
- `00_bootstrap.sql` creates only the account settings and the `HOL_USER` login with its
  token. Everything else the attendee builds by prompting you.
- `MFG.RAW.SIMULATOR_CONTROL` is the simulator's control plane: a standard table holding
  `MODE` (`STEADY` | `INCIDENT` | `REINSPECT`) and `UPDATED_AT`. Writing to it is how Part 5
  changes the plant. **The producer starts once in Part 2 and is never stopped or restarted
  until cleanup.**
- Setup check: if the attendee asks to test the connection and confirm this skill is loaded,
  run `SELECT CURRENT_ACCOUNT() AS account, CURRENT_USER() AS user, CURRENT_ROLE() AS role, CURRENT_REGION() AS region;`,
  report the values (expect `HOL_USER`, `ACCOUNTADMIN`), and confirm both
  `streaming-cdc-iceberg-lab` and the satellite `iceberg-external-read` are present.

## Iceberg setup — do this before creating any table

Set all three parameters on **both** schemas, plus the version default on the database:

```sql
ALTER DATABASE MFG SET ICEBERG_VERSION_DEFAULT = 3;
ALTER SCHEMA MFG.RAW SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.RAW SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.RAW SET ICEBERG_VERSION_DEFAULT = 3;
-- and the same three on MFG.ANALYTICS
```

`MFG.ANALYTICS` needs them even though no plain Iceberg table is ever created there: a
`CREATE DYNAMIC ICEBERG TABLE` takes its version from the **target** schema.

**HARD RULE: issue `USE SCHEMA MFG.RAW;` (or `MFG.ANALYTICS`) immediately before every
Iceberg `CREATE`.** For a plain `CREATE ICEBERG TABLE` the version resolves from the session's
current schema, so without it the table silently lands on v2 while `SHOW PARAMETERS` still
reports 3. **Never cite `SHOW PARAMETERS` as proof that v3 is working** — only a created table's
`iceberg_table_format_version` counts, which is what the preflight checks. A table that came out
v2 must be recreated. Run the preflight after creating tables and before building on them.

## Constraints that change what you generate

- `MODE()` is a hard `CREATE` error under change tracking. Count at defect grain and rank at
  read time.
- `APPROX_PERCENTILE` forces a FULL refresh. Do not use it.
- Pin `TARGET_LAG = '1 minute'` on every layer. `DOWNSTREAM` inherits from the consumer, so a
  "1 minute" pipeline can silently run at the consumer's lag.
- Iceberg rejects `OBJECT` / `OBJECT_AGG` output (v2 and v3 alike), bare `NUMBER` (use
  `NUMBER(38,0)`), `VARCHAR(n)` below max length (use `STRING`), and column `DEFAULT` clauses.
  `VARIANT` requires v3.
- Keep the `::TIMESTAMP_NTZ(6)` cast on every `TIME_SLICE()`. It returns its input's type, and
  scale 9 is rejected on v2.
- **Cast the Gold layer's aggregates: `COUNT(*)` and `SUM()` to `NUMBER(9,0)`,
  `ROUND(…, 2)` to `NUMBER(5,2)`.** Inferred precision reaches `NUMBER(29,2)` for a percentage
  bounded at 100, and that width lands in the Iceberg schema an external engine reads in Part 6.
  `solutions/04_dynamic_tables.sql` carries every cast.
- Dynamic Iceberg tables: no `IF NOT EXISTS`, no `ALTER DYNAMIC ICEBERG TABLE`, no backfill. The
  effective catalog must be `SNOWFLAKE`, which the schema defaults already ensure — do not add a
  `CATALOG` clause to the DDL.
- Snowpipe Streaming into Iceberg: no partitioned tables, no schema evolution.
- Telemetry is queryable in **~30 s**, not instantly. State it as the observed baseline and
  name no mechanism — in particular not `MAX_CLIENT_LAG`, which is a Snowpipe Streaming
  *Classic* property and does not apply here.

## Object Model

### `MFG.RAW.QUALITY_INSPECTIONS` — Iceberg v3, the CDC destination

Deliberately **not** Iceberg: it takes UPDATEs and DELETEs continuously, which is what a
change feed does to its destination.

`INSPECTION_ID STRING` (replication key), `UNIT_ID STRING`, `LINE STRING`
(WELD | PAINT | ASSEMBLY), `SKU STRING`, `STATUS STRING` (PASS | FAIL),
`DEFECT_CODE STRING` (NULL on PASS), `STATION_ID STRING`, `OPERATOR_ID STRING`,
`EVENT_TS TIMESTAMP_NTZ`, `UPDATED_TS TIMESTAMP_NTZ`, plus the connector's bookkeeping:
`_SNOWFLAKE_INSERTED_AT`, `_SNOWFLAKE_UPDATED_AT`, `_SNOWFLAKE_DELETED BOOLEAN`.

`_SNOWFLAKE_DELETED` is a **soft** delete — the connector flags rows, never removes them.
Filtering it is the attendee's job downstream.

Defect codes: `WELD_POROSITY`, `WELD_MISALIGN`, `PAINT_RUN`, `PAINT_ORANGE_PEEL`,
`ASSY_TORQUE`, `ASSY_MISSING_PART`.

### `MFG.RAW.STATION_TELEMETRY` — Iceberg, Snowpipe Streaming target

`STATION_ID STRING`, `LINE STRING`, `METRIC STRING`, `VALUE DOUBLE`,
`EVENT_TS TIMESTAMP_NTZ`. Metrics: `weld_current`, `booth_humidity`, `booth_temp`,
`torque_nm`.

Create it with **no** `CATALOG`, `EXTERNAL_VOLUME` or `ICEBERG_VERSION` clause — all three
come from the schema defaults, and the preflight confirms they did. Snowpipe Streaming
auto-creates the pipe `STATION_TELEMETRY-STREAMING`; there is never a `CREATE PIPE` here.

### The CDC journal — created BY THE CONNECTOR

**The attendee never creates this, and neither do you.** `producer/cdc_simulator.py` creates
the journal, its `APPEND_ONLY` stream and the destination table on first run; its
`JOURNAL_DDL`, `STREAM_DDL` and `DESTINATION_DDL` constants are the only live copies of that
DDL. Objects: `MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1` and `..._STREAM`.

**There is no task, and you must not create one.** The connector's merge processor runs in the
connector runtime and issues the MERGE over its own connection, on a one-minute CRON
*eligibility gate*; the producer does the same. If an attendee asks you to schedule the merge,
explain that a task would misrepresent how the product works.

Attendees **inspect** the journal; they never build Dynamic Tables on it. Build on
`QUALITY_INSPECTIONS`. Event types, MERGE semantics, the query tag and `SF_METADATA` are in
[`docs/cdc-internals.md`](../../../../docs/cdc-internals.md).

### The Dynamic Iceberg Table DAG — all four INCREMENTAL, `TARGET_LAG = '1 minute'`

Emit the DDL from `solutions/04_dynamic_tables.sql` verbatim. Shapes:

1. **`INSPECTIONS_ACTIVE`** — `FROM QUALITY_INSPECTIONS WHERE NOT _SNOWFLAKE_DELETED`, adding
   `IS_SCRAP = IFF(STATUS='FAIL',1,0)`. Pass through the **business** columns only —
   `INSPECTION_ID`, `UNIT_ID`, `LINE`, `SKU`, `STATUS`, `DEFECT_CODE`, `STATION_ID`,
   `OPERATOR_ID`, `EVENT_TS`, `UPDATED_TS` — and **not** the `_SNOWFLAKE_*` bookkeeping columns.
2. **`STATION_HEALTH`** — from `STATION_TELEMETRY`, grouped by `STATION_ID, LINE, METRIC, BUCKET`,
   giving `READINGS`, `AVG_VALUE`, `MAX_VALUE`.
3. **`YIELD_BY_LINE_5MIN`** — `INSPECTIONS_ACTIVE` LEFT JOIN `STATION_HEALTH` on `LINE` +
   matching `BUCKET` + `METRIC='booth_humidity'`, grouped by `LINE, BUCKET`, giving `UNITS`,
   `SCRAP_UNITS`, `FIRST_PASS_YIELD_PCT`, `AVG_BOOTH_HUMIDITY`. **This is the two-source join
   and it holds INCREMENTAL.** `FIRST_PASS_YIELD_PCT` is
   `ROUND(100 * (COUNT(*) - SUM(IS_SCRAP)) / COUNT(*), 2)` — keep the `ROUND(…, 2)`, or the
   column becomes full-precision division and changes the numbers the agent reads back.
4. **`DEFECT_COUNTS_5MIN`** — from `INSPECTIONS_ACTIVE`, grouped by
   `LINE, BUCKET, COALESCE(DEFECT_CODE,'NONE')`, giving `N`.

`AVG_BOOTH_HUMIDITY` is legitimately NULL for WELD and ASSEMBLY — booth humidity is a
paint-booth metric. Say so if asked; do not "fix" it.

### `MFG.ANALYTICS.PLANT_FLOOR_SV` — semantic view

Emit `solutions/05_semantic_view.sql` verbatim; change only synonyms and comments. Three
logical tables: `yield`, `defects`, `stations`. Four syntax rules — if a `CREATE` fails,
re-emit the file rather than improvising the grammar:

1. Clause order is fixed: `TABLES` → `RELATIONSHIPS` → `FACTS` → `DIMENSIONS` → `METRICS`
2. Tables bind with `AS`, never `=`
3. Synonyms use `WITH SYNONYMS = (...)`, never a bare `SYNONYMS = (...)`
4. Metrics are alias-qualified and defined with `AS` — `yield.total_units AS SUM(yield.units)`

### `MFG.ANALYTICS.CASCADE_PLANT_ANALYST` — Cortex Agent

Emit `solutions/06_agent.sql` verbatim. Four rules:

- **Dollar-quote with `$$`, never a named tag** like `$spec$` — Cortex Code's SQL path rejects
  named tags with `unexpected '$spec'`. The spec JSON never contains `$$`.
- **`models.orchestration` must be `"auto"`.** A pinned model can fail with *"not an allowed
  model for Agent"*. Pin one afterwards in Snowsight under **Configuration → Model** if wanted;
  the id is `claude-sonnet-4-5`, not `claude-4-sonnet`.
- **Keep `execution_environment` in `tool_resources`.** Without it `CREATE AGENT` succeeds and
  every question then fails with `internal error`, code 391920.
- **To change the agent, re-run the whole `CREATE OR REPLACE AGENT` statement.** A
  workspace-file edit path fails — this agent comes from SQL, not a workspace.

The three questions to send the attendee to are in
[`docs/agent_questions.md`](../../../../docs/agent_questions.md). Point them there rather than
retyping them.

## Workflow

Follow the attendee's lead. Use the **Part** numbers they see in the README.

**Setup D (pre-work) — local environment.** Detect the OS first and use the matching paths.
macOS/Linux: `python3 -m venv .venv`, then `.venv/bin/python` and `.venv/bin/pip`. Windows:
`python -m venv .venv` (not `python3`), then `.venv\Scripts\python.exe` and
`.venv\Scripts\pip.exe` — never `uv`, never the POSIX paths. Install **both** requirement sets:
`<venv-pip> install -r producer/requirements.txt -r external/requirements.txt` — the second is
PyIceberg for Part 6, installed now because nothing may install during the session. Confirm with
`<venv-python> -c "import pyiceberg, snowflake.connector"` before calling Setup D done.

Then **verify** the attendee's `profile.json` in the repo root. **You do not create it** — they write
it in Setup B from `profile.example.json`, because the PAT is theirs and must not pass through
chat. Check it **inside a shell command** (e.g. a `python3 -c` one-liner) so secrets are never
printed and never read into your context: confirm the file parses as JSON, that `user` is
`HOL_USER`, that `personal_access_token` is non-empty (this is the field the producer reads —
not `token`), and that neither `MYORG` nor `PASTE_YOUR` survives anywhere in it. Report only
which check failed, never a value. If it is missing, ask the attendee to do Setup B step 3.

`url` may be empty; the producer derives it from `account` on first run and writes it back. Do not
fill it yourself, and **do not "fix" `account` from the `cortex` CLI or `snowflake_connections_list`**
— those resolve to the CLI's default connection, often a different account entirely. If `account`
looks wrong, have the attendee re-run
`SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();` on the active connection and
paste the result in themselves.

**Part 1 — environment and landing tables.** Create `MFG`, schemas `RAW` and `ANALYTICS`, the
database version default, the three Iceberg defaults on both schemas, `HOL_WH`, and
`ALTER USER HOL_USER SET DEFAULT_WAREHOUSE = HOL_WH` (an agent caller resolves the default
warehouse from the user, not the session). Then exactly two tables: `STATION_TELEMETRY` and
`SIMULATOR_CONTROL`, with `USE SCHEMA` before the Iceberg create. Checkpoint P.

**Do not create `QUALITY_INSPECTIONS`, the journal or its stream** — the connector creates all
three on first run. `SIMULATOR_CONTROL` is small but load-bearing: without it the producer logs
`[control] read failed` and Part 5 fails on a missing table.

**Part 2 — start the producer, then inspect the journal.** Start it **in the background** (Bash
`run_in_background`) with the venv interpreter, so it keeps streaming while later layers build:
`<venv-python> producer/main.py --cdc --telemetry`.
Checkpoint J, then Checkpoint I. Then walk the change events, the three `EVENT_TYPE` shapes and
above all the **merge gate** — the journal always leads the destination, and each MERGE fires at
second :00 and finishes in a second or two. Checkpoint G-gate. Close by telling the attendee to
build on `QUALITY_INSPECTIONS`, not the journal. Do not skip this step. `SF_METADATA`'s
`PARSE_JSON` behaviour and the `QUERY_TAG` audit are not core — cover them only if asked, and
point at `docs/cdc-internals.md`.

**Part 3 — the four Dynamic Tables.** `INSPECTIONS_ACTIVE` and `STATION_HEALTH` first
(Checkpoint D), then `YIELD_BY_LINE_5MIN` and `DEFECT_COUNTS_5MIN` (Checkpoint Y). Both prompts
specify source, grain, logic and column names, including the join keys and the `booth_humidity`
metric; build what they specify and confirm against `solutions/04_dynamic_tables.sql`. Then show
refresh history, and demonstrate the `MODE()` failure only if asked.

**Part 4 — semantic view and agent.** Emit both verbatim from `solutions/`. Then send the
attendee to chat with the agent in **Snowsight → AI & ML → Agents → Cascade Plant Analyst**, on
the detail page's chat panel — not here. They do **not** need to Publish.

**Part 5 — the dashboard, the incident, then the recovery.** Deploy the dashboard first, before
`INCIDENT` — see *Deploying the live dashboard* below. Then **do not restart the producer.** Change
the world instead:

```sql
INSERT INTO MFG.RAW.SIMULATOR_CONTROL (MODE, UPDATED_AT)
  VALUES ('INCIDENT', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
```

The newest row wins and the producer picks it up within ~10 s. Tell the attendee to watch the
producer's own log: `booth_humidity` climbs away from 44 within seconds, and
`[cdc] PAINT defect rate -> 26%` follows ~90 s later. That is the cascade, visible before any
query. Have them stopwatch each layer, then ask the agent *why*. Checkpoint X.

Then the same statement with `'REINSPECT'`. This fixes the booth (humidity returns to ~44) and
starts a **bounded** burst of re-inspections — about 40% of the failed backlog over ~3 minutes,
then back to normal cadence on its own. Inspectors overturn failed frames to PASS, so yield goes
UP for buckets that already reported, but it does **not** reach 100%: some frames really are
scrap. If yield pins at exactly 100% with an empty `DEFECT_COUNTS_5MIN`, something is wrong.
Checkpoint R.

If an attendee asks you to stop or restart the producer to change its behaviour, explain that a
real connector runs continuously — an incident changes the data at the source, it does not bounce
the connector — and that restarting risks HTTP 409 `ERR_CHANNEL_HAS_UNCOMMITTED_DATA` from
reopening a channel too soon.

**Deploying the live dashboard — Part 5's first step.** For *deploy the dashboard*, *the plant floor
dashboard*, or *I want to see it live*. **Deploy the file that ships in `dashboard/`. Never
generate, rewrite or "improve" it** — see *Not attendee build steps*. `PUT` works over the SQL
connection, so no `snow` CLI and no local server. **`dashboard/snowflake.yml` also describes this
app, and you should not use it.** It is a valid `snow streamlit deploy` definition kept for anyone
working outside Cortex Code, but that path needs the CLI installed and its own connection, neither
of which the lab sets up. Deploy with the four statements below:

```sql
CREATE STAGE IF NOT EXISTS MFG.ANALYTICS.DASHBOARD_STAGE DIRECTORY = (ENABLE = TRUE);

PUT file://<repo>/dashboard/streamlit_app.py @MFG.ANALYTICS.DASHBOARD_STAGE/plant_floor
  AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

PUT file://<repo>/dashboard/environment.yml @MFG.ANALYTICS.DASHBOARD_STAGE/plant_floor
  AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

CREATE OR REPLACE STREAMLIT MFG.ANALYTICS.PLANT_FLOOR_LIVE
  FROM '@MFG.ANALYTICS.DASHBOARD_STAGE/plant_floor'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = HOL_WH
  TITLE = 'Plant Floor — Live Quality';
```

`AUTO_COMPRESS = FALSE` is required — the default gzips the file and the app then fails to load.
**Upload `environment.yml` as well**, into the same stage path: it pins the Streamlit version, and
without it the app resolves a build with no `st.fragment` and dies on an `AttributeError`. Use an
absolute path in each `PUT`, resolved for the attendee's OS. Do **not** add `RUNTIME_NAME` or
`COMPUTE_POOL`: this is a warehouse-runtime app, and a container runtime waits on a compute pool
node before it serves.

**Run all four statements every time, including when the app already exists.** A `STREAMLIT` serves an
immutable version snapshot, not the stage: `DESCRIBE STREAMLIT` shows the app running from
`snow://streamlit/.../versions/version$1/` while the stage is only its `source_location_uri`. So `PUT`
on its own updates the source and changes nothing the app runs — no error, no warning, and the app
keeps serving the old code. The `CREATE OR REPLACE` is what re-snapshots. If someone reports that an
edit has not appeared, re-run the block rather than re-uploading, and confirm with
`LIST 'snow://streamlit/MFG.ANALYTICS.PLANT_FLOOR_LIVE/versions/version$1/'` — the size and md5 there
are what is actually being served.

Run the column contract first. The app addresses `LINE`, `BUCKET`, `UNITS`, `SCRAP_UNITS`,
`FIRST_PASS_YIELD_PCT` and `AVG_BOOTH_HUMIDITY` on `YIELD_BY_LINE_5MIN`, and `LINE`, `BUCKET`,
`DEFECT_CODE` and `N` on `DEFECT_COUNTS_5MIN`. Then tell them to open it at **Snowsight →
Projects → Streamlit → Plant Floor — Live Quality**. It refreshes itself every 30 s; the
**Auto-refresh** toggle stops that and **Refresh now** re-queries on demand.

An empty defect panel means no defects in the last 15 minutes, which is correct in steady state
before the incident. Deploy it **before** setting mode to `INCIDENT` so the room watches humidity
rise and yield fall on the same time axis.

**Part 6 — read it from outside.** Generate nothing: run `external/read_iceberg.py` with the venv
interpreter. PyIceberg is already installed from Setup D. The satellite skill
`iceberg-external-read` owns this Part, including its auth traps — defer to it. Checkpoint E.

**Cleanup.** Run **Block 1** of `solutions/09_cleanup.sql` (suspend the four Dynamic Tables and
`HOL_WH`) and report `scheduling_state`. Stop the producer first — and stop it yourself if you
started it, since that is what actually stops the spend. Run **Block 2** (drop everything) only
if the attendee explicitly asks, and only once the producer is stopped: dropping the journal
under an open channel produces errors that look like a lab failure. Never run the commented
Block 3 — it is Snowsight-only and drops the user you are connected as.

## Checkpoints

- **P (preflight)** — `solutions/02_preflight.sql`. `aws_ok`, `cortex_ok`, `raw_iceberg_ok` and
  `analytics_iceberg_ok` must all be TRUE, and every Iceberg object must report `is_v3 = TRUE`.
  Do not proceed past a FALSE.
- **J (the connector's own objects)** — on first run the producer logs
  `[connector] destination table ready` / `journal ready` / `journal stream ready`. Confirm what
  *it* built:
  ```sql
  SHOW ICEBERG TABLES LIKE 'QUALITY_INSPECTIONS_JOURNAL%' IN SCHEMA MFG.RAW;
  SHOW STREAMS LIKE '%_JOURNAL_%_STREAM' IN SCHEMA MFG.RAW;
  ```
  The journal must report Iceberg format version 3 and the stream `mode = APPEND_ONLY`.
  `QUALITY_INSPECTIONS` must exist and must **not** be Iceberg.
- **I (ingest)** — both feeds landing.
  ```sql
  SELECT COUNT(*) AS journal_events, COUNT_IF(EVENT_TYPE='IncrementalUpdateRows') AS updates
  FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1;
  SELECT COUNT(*) AS destination_rows FROM MFG.RAW.QUALITY_INSPECTIONS;
  SELECT COUNT(*) AS telemetry_rows,
         DATEDIFF('second', MAX(EVENT_TS), CURRENT_TIMESTAMP()) AS seconds_ago
  FROM MFG.RAW.STATION_TELEMETRY;
  ```
  All climb on re-run. Telemetry `seconds_ago` is ~30 s, expected. The destination trails the
  journal by up to a minute — also expected, and that is the next checkpoint.
- **G-gate (the merge gate)** — `journal_inserts` exceeds `destination_rows`, and `QUERY_HISTORY`
  filtered on the connector's `QUERY_TAG` shows one MERGE per minute, each starting at second :00
  and completing in a second or two. Both queries are in `solutions/03_journal_inspection.sql`.
  The gap is the gate, not the merge.
- **D (layer 1)** — `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS`: every row must read
  `refresh_mode = INCREMENTAL`, `is_iceberg = true`, and an empty `refresh_mode_reason`.
- **Y (gold)** — yield by line for recent buckets. Three lines, each around 95–99% in steady
  state, with humidity populated for PAINT only.
- **X (incident)** — PAINT `FIRST_PASS_YIELD_PCT` drops well below 90, into the **70s or 80s**
  depending on where in the 5-minute bucket the incident started; a fully affected bucket sits in
  the **high 70s**, and if the mode change lands near a bucket boundary the first affected bucket
  is already there. WELD and ASSEMBLY stay around 96–98%. `AVG_BOOTH_HUMIDITY` for PAINT climbs
  from ~44 into the 60s–70s, and `PAINT_RUN` dominates the defect counts.
- **R (recovery)** — PAINT yield rises again and
  `COUNT_IF(_SNOWFLAKE_UPDATED_AT > _SNOWFLAKE_INSERTED_AT)` on `QUALITY_INSPECTIONS` climbs. The
  Dynamic Tables stay INCREMENTAL.
- **E (external read)** — `external/read_iceberg.py` prints Iceberg format version **3**, a
  storage path under Snowflake's managed bucket (`s3://sfc-…-customer-interop-fs-…`), rows from
  `YIELD_BY_LINE_5MIN`, and a **smaller** row count after predicate pushdown on `LINE == 'PAINT'`.
  No warehouse ran the scan.

## Statements to run verbatim from `solutions/progress.sql`

**"Where am I?"** — for where they are, what they have built, what is missing, or whether
anything is flowing, run the **inventory** statement verbatim. Reading it for them: everything
through their current Part should say `built`; `QUALITY_INSPECTIONS` having fewer rows than the
journal is the merge gate, not a fault; `YIELD_BY_LINE_5MIN` holding single-digit rows is correct
(three lines × elapsed 5-minute buckets); `built` with 0 rows and no growth means the producer is
not running. The agent is not in that query — check it with
`SHOW AGENTS LIKE 'CASCADE_PLANT_ANALYST' IN SCHEMA MFG.ANALYTICS;`.

**The refresh state** — run the `THE REFRESH STATE` statement whenever a derived table looks stale
while `MFG.RAW` keeps growing, and always before concluding the producer is at fault. All four must
read `ACTIVE`. A **suspended** Dynamic Table still reports `built` in the inventory with the row
count it held when it stopped, so row counts alone cannot distinguish frozen from idle. Cleanup
Block 1 suspends all four, so any account that ran it resumes with rows landing in
`QUALITY_INSPECTIONS` and every layer above it holding still. Resume upstream first
(`INSPECTIONS_ACTIVE`, `STATION_HEALTH`, then the two Gold tables) with
`ALTER DYNAMIC TABLE MFG.ANALYTICS.<name> RESUME;`, allowing about a minute per layer.

**The column contract** — for *check the columns*, *did my Dynamic Tables come out right*, or any
request to verify Part 3 beyond refresh mode, run the `THE COLUMN CONTRACT` statement verbatim.
Every row must read `ok`. On `-- MISSING COLUMN --`, re-issue the Part 3 prompt for **that table
only**, naming the column explicitly — a Dynamic Table's columns come from its query, so do not
`ALTER` the name. On `-- WRONG TYPE --`, it is usually `IS_SCRAP` as `BOOLEAN`. Run this **before**
Part 4: the semantic view addresses these columns by name, so a mismatch surfaces there and points
at the view rather than the table that caused it.

## Stopping points

- No Iceberg `CREATE` without `USE SCHEMA` on the line before it, and none before the storage
  defaults are set.
- Do not proceed past Checkpoint P with any FALSE or any object on v2. Recreate a v2 table.
- Confirm `STATION_TELEMETRY` and `SIMULATOR_CONTROL` exist and the preflight passed before
  starting the producer.
- Never stop or restart the producer to change its behaviour — write to `SIMULATOR_CONTROL`.
- Never create a Snowflake task for the merge.
- Run each layer's Checkpoint before moving to the next.

## References

Every fact has exactly one home. This skill carries no copies.

- `solutions/*.sql` — verbatim DDL, one file per Part. Emit only its `CREATE` statements plus the
  `USE ROLE` / `USE WAREHOUSE` / `USE SCHEMA` lines above them; never the checkpoint `SELECT`s as
  part of a build step, and never a commented-out block. Generate the DDL — do not tell the
  attendee to run the file instead. `solutions/` is their fallback, not your shortcut.
- `solutions/progress.sql` — the inventory, refresh-state and column-contract statements.
- `producer/cdc_simulator.py` — the connector's own DDL and the MERGE it issues.
- `docs/cdc-internals.md`, `docs/agent_questions.md`, `docs/troubleshooting.md`.

**Not attendee build steps.** Do not offer to generate these: `external/read_iceberg.py` (Part 6,
owned by the `iceberg-external-read` skill) and `dashboard/streamlit_app.py`. The dashboard file is
**deployed, never generated** — writing a Streamlit app from a prompt takes several correction
cycles, which is why the shipped file exists. Deploy it at the top of Part 5; if asked to change it,
deploy it as it stands first.

## Output

The Cascade Cycleworks pipeline: a CDC journal and the producer-issued gated MERGE,
`QUALITY_INSPECTIONS`, `STATION_TELEMETRY`, four Dynamic Iceberg Tables, `PLANT_FLOOR_SV`, and an
agent that can explain a two-phase quality incident by reading across both sources.
