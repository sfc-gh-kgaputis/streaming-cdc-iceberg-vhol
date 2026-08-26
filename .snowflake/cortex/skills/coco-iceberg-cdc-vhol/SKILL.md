---
name: coco-iceberg-cdc-vhol
description: "Build the Cascade Cycleworks real-time manufacturing pipeline for the CDC to Dynamic Tables to Iceberg VHOL. Loads the exact object model, Iceberg v3 settings, and DDL patterns so short prompts produce consistent objects. Use when: building the CDC or telemetry pipeline, the Iceberg tables, the Dynamic Table DAG, the semantic view, the plant analyst agent, or setting up the data producer for this lab. Triggers: iceberg cdc vhol, cascade cycleworks, quality inspections, station telemetry, inspections_active, yield by line, plant analyst, defect counts, openflow simulator."
---

# Cascade Cycleworks Iceberg CDC VHOL

You are helping a workshop attendee build a real-time manufacturing pipeline on
Snowflake. The scenario: **Cascade Cycleworks**, a bicycle frame manufacturer,
replicates quality-inspection data out of its MES with change data capture and
streams sensor telemetry off the line. Dynamic Iceberg Tables refine both
continuously, and an AI agent answers questions about the plant floor.

**Golden rule:** when the attendee asks to build a layer, produce the object with
the EXACT names, columns, and settings in the Object Model below. Do not rename
columns, change the grain, or alter target lag. Consistency is what keeps every
attendee's pipeline working through the whole lab. Always use `CREATE OR REPLACE`
so a re-run is safe. After creating an object, run its Checkpoint and report the
result.

## Fixed context

- Database `MFG`. Two schemas: `RAW` (both landing zones — the CDC destination,
  its journal and stream, and the streaming telemetry table) and `ANALYTICS`
  (everything derived — all four Dynamic Tables, the semantic view, the agent).
  Warehouse `HOL_WH`, Gen2 XSMALL.
- `MFG.RAW.SIMULATOR_CONTROL` is the simulator's **control plane** — a standard table
  holding `MODE` (`STEADY` | `INCIDENT` | `REINSPECT`) and `UPDATED_AT`. Writing to it is
  how Part 5 changes the plant. **The producer is started once, in Part 2, and is never
  stopped or restarted until cleanup.**
- **Everything is Apache Iceberg v3 on Snowflake-managed storage.** Attendee
  accounts have no connected cloud storage, so they cannot create an external
  volume. `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'`, `CATALOG = 'SNOWFLAKE'`, and
  never a `BASE_LOCATION`.
- The account is set to UTC by `00_bootstrap.sql`. The producer emits UTC event
  times, so all freshness and lag math uses `CURRENT_TIMESTAMP()` and lines up.
  Do not mix in local-timezone timestamps.
- `00_bootstrap.sql` creates ONLY the account settings and the `HOL_USER` login
  with its token. Everything else the attendee builds by prompting you.
- Setup check: if the attendee asks to test the connection and confirm this skill
  is loaded, run
  `SELECT CURRENT_ACCOUNT() AS account, CURRENT_USER() AS user, CURRENT_ROLE() AS role, CURRENT_REGION() AS region;`,
  report the values (expect user `HOL_USER`, role `ACCOUNTADMIN`), and confirm the
  `coco-iceberg-cdc-vhol` skill is active (you are running it).

## The load-bearing Iceberg defaults — and the trap in them

Set these before creating any table, on **both** schemas, plus the database:

```sql
ALTER DATABASE MFG SET ICEBERG_VERSION_DEFAULT = 3;
ALTER SCHEMA MFG.RAW SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.RAW SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.RAW SET ICEBERG_VERSION_DEFAULT = 3;
-- and the same three on MFG.ANALYTICS
```

**HARD RULE: issue `USE SCHEMA MFG.RAW;` (or `MFG.ANALYTICS`) immediately before every
Iceberg `CREATE`.** This is not stylistic. Measured 26 Aug 2026:

- `EXTERNAL_VOLUME` and `CATALOG` resolve from the schema that **contains** the new
  table, as expected.
- `ICEBERG_VERSION_DEFAULT` resolves from the **session's current schema**.

So `CREATE ICEBERG TABLE MFG.RAW.T (...)` issued without `USE SCHEMA MFG.RAW` first
gets the correct volume and catalog but silently lands on **version 2** — while
`SHOW PARAMETERS ... IN SCHEMA MFG.RAW` continues to report `value = 3, level = SCHEMA`.
The parameter is set, reported, and ignored. **Never cite `SHOW PARAMETERS` as proof
that v3 is working**; only a created table's `iceberg_table_format_version` counts.

Consequences of landing on v2, all of which surface far from the cause:
`VARIANT` columns are rejected outright; `TIME_SLICE()`'s `TIMESTAMP_NTZ(9)` is
rejected; and **`CREATE DYNAMIC ICEBERG TABLE` has no `ICEBERG_VERSION` clause at
all**, so for the Dynamic Table layer there is no per-statement override — it inherits
or it is wrong.

There is no in-place v2 → v3 upgrade. A table that came out v2 must be recreated.
Always run the preflight (`solutions/02_preflight.sql`) after creating tables, and
before building anything on top of them.

## Object Model (single source of truth)

### MFG.RAW.QUALITY_INSPECTIONS — standard table, the CDC destination

Deliberately a **standard** table, not Iceberg: it takes UPDATEs and DELETEs
continuously, which is the entire point of a change feed.

Columns: `INSPECTION_ID STRING` (replication key), `UNIT_ID STRING`, `LINE STRING`
(WELD | PAINT | ASSEMBLY), `SKU STRING`, `STATUS STRING` (PASS | FAIL),
`DEFECT_CODE STRING` (NULL on PASS), `STATION_ID STRING`, `OPERATOR_ID STRING`,
`EVENT_TS TIMESTAMP_NTZ`, `UPDATED_TS TIMESTAMP_NTZ`,
`_SNOWFLAKE_INSERTED_AT TIMESTAMP_NTZ`, `_SNOWFLAKE_UPDATED_AT TIMESTAMP_NTZ`,
`_SNOWFLAKE_DELETED BOOLEAN`.

The `_SNOWFLAKE_*` columns are what the Openflow connector maintains.
`_SNOWFLAKE_DELETED` is a **soft** delete — the connector never removes rows, it
flags them. Filtering it is the attendee's job downstream.

Defect codes: `WELD_POROSITY`, `WELD_MISALIGN`, `PAINT_RUN`, `PAINT_ORANGE_PEEL`,
`ASSY_TORQUE`, `ASSY_MISSING_PART`.

### MFG.RAW.STATION_TELEMETRY — Iceberg, Snowpipe Streaming target

Columns: `STATION_ID STRING`, `LINE STRING`, `METRIC STRING`, `VALUE DOUBLE`,
`EVENT_TS TIMESTAMP_NTZ`.

Metrics: `weld_current`, `booth_humidity`, `booth_temp`, `torque_nm`.

Create it with **no** `CATALOG`, `EXTERNAL_VOLUME`, or `ICEBERG_VERSION` clause —
all three come from the schema defaults, and proving that is the point of the
preflight check. Snowpipe Streaming auto-creates a default pipe named
`STATION_TELEMETRY-STREAMING`; there is never a `CREATE PIPE` in this lab.

### The Dynamic Iceberg Table DAG — all four INCREMENTAL, TARGET_LAG '1 minute'

Emit the DDL from `references/object_model.md` verbatim. Shapes:

1. **`INSPECTIONS_ACTIVE`** — `FROM QUALITY_INSPECTIONS WHERE NOT _SNOWFLAKE_DELETED`,
   passes columns through, adds `IS_SCRAP = IFF(STATUS='FAIL',1,0)`.
2. **`STATION_HEALTH`** — from `STATION_TELEMETRY`, grouped by
   `STATION_ID, LINE, METRIC, BUCKET`, with `READINGS`, `AVG_VALUE`, `MAX_VALUE`.
3. **`YIELD_BY_LINE_5MIN`** — `INSPECTIONS_ACTIVE` LEFT JOIN `STATION_HEALTH`
   on `LINE` + matching `BUCKET` + `METRIC='booth_humidity'`, grouped by
   `LINE, BUCKET`, giving `UNITS`, `SCRAP_UNITS`, `FIRST_PASS_YIELD_PCT`,
   `AVG_BOOTH_HUMIDITY`. **This is the two-source join and it holds INCREMENTAL.**
4. **`DEFECT_COUNTS_5MIN`** — from `INSPECTIONS_ACTIVE`, grouped by
   `LINE, BUCKET, COALESCE(DEFECT_CODE,'NONE')`, giving `N`.

`AVG_BOOTH_HUMIDITY` is legitimately NULL for WELD and ASSEMBLY — booth humidity
is a paint-booth metric. Say so if the attendee asks; do not "fix" it.

### The CDC journal — Iceberg v3, connector-internal

Two objects: `MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1` and its `APPEND_ONLY`
stream `..._STREAM`. Emit both verbatim from `references/object_model.md`.

**There is no task, and you must not create one.** The Openflow connector does not
create a Snowflake `TASK` — verified against the connector source. Its merge processor
runs inside the connector runtime and issues the MERGE itself over its own Snowflake
connection, with a CRON expression acting as an internal *eligibility gate* (flow
default `0 * * * * ?`, second :00 of every minute). In this lab the **producer** issues
the MERGE the same way, on the same gate. If an attendee asks you to schedule the merge,
explain that a task would misrepresent how the product works.

The connector sets a `QUERY_TAG` on every merge, and so does the producer:
`{"application":"SNOWFLAKE_OPENFLOW","operation":"cdc.merge.full_values","strategy":"full_values_snowflake_managed"}`.
That is how you find the merges in `QUERY_HISTORY`, and it is how a customer would audit
a real deployment. Use it for the merge-gate checkpoint instead of task history.

Column order is fixed — `PRIMARY_KEY__<col>` first, then `PAYLOAD__<col>` for **every**
source column, then `LEAST_/MOST_SIGNIFICANT_POSITION`, `EVENT_TYPE`, `SEEN_AT`,
`SF_METADATA`.

`EVENT_TYPE` literals: `IncrementalInsertRows`, `IncrementalUpdateRows`,
`IncrementalDeleteRows`.

Per-operation semantics that drive the MERGE:
- **UPDATE** — `PRIMARY_KEY__*` is the OLD key, `PAYLOAD__*` the NEW values.
- **DELETE** — every `PAYLOAD__*` is NULL. The key alone identifies the row, which is
  why the MERGE's INSERT branch needs
  `IFF(EVENT_TYPE='IncrementalDeleteRows', PRIMARY_KEY__INSPECTION_ID, PAYLOAD__INSPECTION_ID)`.

The journal carries an explicit `ICEBERG_VERSION = 3` because `SF_METADATA VARIANT`
fails outright on v2, and `ERROR_LOGGING = TRUE` as the connector sets.

**Attendees INSPECT the journal; they never build Dynamic Tables on it.** It is
connector-internal, its schema shifts with the generation counter, and the connector
prunes it. Build on `QUALITY_INSPECTIONS`.

**`SF_METADATA` holds a JSON *string*, not a parsed object** — the connector writes it
that way. `SF_METADATA:offset_token` returns NULL; use
`PARSE_JSON(SF_METADATA::STRING):offset_token`. `TYPEOF(SF_METADATA)` is `VARCHAR`.
This is faithful, not a bug — make it a teaching moment rather than fixing it.

### MFG.ANALYTICS.PLANT_FLOOR_SV — semantic view

Build from the verbatim DDL in `references/object_model.md`. Change only synonyms
and comments. Three logical tables: `yield`, `defects`, `stations`.

### MFG.ANALYTICS.CASCADE_PLANT_ANALYST — Cortex Agent

Build from the verbatim spec in `references/agent_spec.md`.

## Measured constraints — do not rediscover these

| # | Constraint |
|---|---|
| 0 | **`ICEBERG_VERSION_DEFAULT` resolves from the session's current schema, not the target table's schema.** `EXTERNAL_VOLUME` and `CATALOG` resolve from the target schema. Always `USE SCHEMA` first. `SHOW PARAMETERS` is not a valid check — only a created table's `iceberg_table_format_version` is. |
| 1 | `TIME_SLICE()` returns `TIMESTAMP_NTZ(9)`, rejected by Iceberg **v2**, accepted on v3. Keep the `::TIMESTAMP_NTZ(6)` cast anyway — it is free insurance if the version default did not take. |
| 2 | **`MODE()` is a hard `CREATE` error** under change tracking: *"Change tracking is not supported on queries containing the function 'MODE'"*. Never put it in a Dynamic Table. Count at defect grain and rank at read time. |
| 3 | `OBJECT` / `OBJECT_AGG` output is rejected by Iceberg on **v2 and v3 alike**. |
| 4 | Bare `NUMBER` is rejected — use `NUMBER(38,0)`. |
| 5 | `VARCHAR(n)` below max length is rejected — use `STRING`. |
| 6 | `VARIANT` requires Iceberg v3. |
| 7 | Column `DEFAULT` clauses are rejected on v2 and gated on v3. The producer supplies all timestamps explicitly. |
| 8 | `APPROX_PERCENTILE` forces a FULL refresh. Avoid it. |
| 9 | Pin `TARGET_LAG` on every layer. `DOWNSTREAM` inherits from the consumer, so a "1 minute" pipeline can silently run at the consumer's lag. |
| 10 | Snowpipe Streaming + Iceberg: no partitioned tables, no schema evolution, no length-constrained VARCHAR. |
| 11 | `MAX_CLIENT_LAG` defaults to **30 s** for Iceberg targets, not ~1 s. This is deliberate, for Parquet file sizing. Expect ~30 s telemetry visibility, not instant. |
| 12 | Dynamic Iceberg tables: no `IF NOT EXISTS`, no `ALTER DYNAMIC ICEBERG TABLE`, `CATALOG` must be `'SNOWFLAKE'`, no backfill. |

## Workflow

Follow the attendee's lead. Each step maps to one prompt. The **Part** on each step
is the Part number the attendee sees in the README — use their vocabulary, not the
step numbers here, when you talk to them.

0. **Producer setup — Setup D, pre-work** — two tasks the attendee will prompt for:

   - **Detect the OS first.** Check the platform before running any shell command
     and use the matching interpreter paths. This lab runs on macOS, Linux, and
     Windows, and only the Python invocation differs:
     - macOS / Linux: create with `python3 -m venv .venv`; the venv interpreter
       is `.venv/bin/python` and pip is `.venv/bin/pip`.
     - Windows: create with `python -m venv .venv` (use `python`, not `python3`);
       the interpreter is `.venv\Scripts\python.exe` and pip is
       `.venv\Scripts\pip.exe`. Do NOT use `uv` or the POSIX `.venv/bin/...`
       paths on Windows.
   - **Install deps:** create the venv at the repo root and install into it
     (`<venv-pip> install -r producer/requirements.txt`). macOS Homebrew Python
     is externally managed (PEP 668), so a venv is required there. Always run the
     producer with the venv interpreter. Two packages, a few seconds.
   - **Build `producer/profile.json`:** `user` is always `HOL_USER` (the bootstrap
     user owns the token), so set it literally, do not query it. Get `account` by
     running SQL on the active connection:
     `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account;`
     **Do NOT use `snowflake_connections_list` or shell out to the `cortex` CLI
     for the account** — those resolve to the CLI's default connection, which is
     often a different account entirely, NOT the trial connection active here.
     Derive `url` as `https://<account>.snowflakecomputing.com:443`, and write
     `producer/profile.json` matching `producer/profile.example.json` with
     `authorization_type: "PAT"`. Fill `personal_access_token` by reading
     `secret.pat` (repo root) **inside a shell command** — e.g. a `python3 -c`
     one-liner that opens the file and writes the JSON — so the token is NEVER
     printed to chat and NEVER read into your context. Do not echo the token. If
     `secret.pat` is missing, ask the attendee to paste their `HOL_PAT` token
     into it first.

1. **Environment + tables — Part 1** — create `MFG`, schemas `RAW` and `ANALYTICS`, the database-level
   version default, **the three Iceberg defaults on both schemas**, `HOL_WH`, then
   `QUALITY_INSPECTIONS`, `STATION_TELEMETRY` **and `SIMULATOR_CONTROL`**. `USE SCHEMA`
   before each Iceberg create. Then run the preflight (Checkpoint P).

   `SIMULATOR_CONTROL` is easy to forget because the attendee's prompt only mentions
   the environment and the landing tables. Create it anyway — Part 5 writes to it, and
   without it the producer logs `[control] read failed` and Part 5's prompt fails on a
   missing table.

2. **CDC journal — Part 1** — create the journal table and its `APPEND_ONLY` stream. Two objects,
   verbatim from `references/object_model.md`. **No task** — the producer issues the
   MERGE, because that is what the connector does. Checkpoint J.

3. **Start the producer — Part 2** — **in the background** (Bash `run_in_background`) with the
   venv interpreter, so it keeps streaming while later layers build:
   `<venv-python> producer/producer.py --profile producer/profile.json --cdc --telemetry`
   Then Checkpoint I. For the incident (step 7) and the recovery (step 8), stop the
   running producer and restart it with the extra flag.

4. **Inspect the journal — Part 2** — walk the change events, the three `EVENT_TYPE` shapes, the
   `SF_METADATA` `PARSE_JSON` quirk, and above all the **merge gate**: the journal always
   leads the destination, and `QUERY_HISTORY` filtered on the connector's `QUERY_TAG`
   shows one MERGE per minute at second :00, each finishing in a second or two.
   Checkpoint G-gate. Do not skip this step — it is the best lesson in the architecture.

5. **Layer 1 — Part 3** — `INSPECTIONS_ACTIVE` and `STATION_HEALTH`. Checkpoint D.

6. **Gold — Part 3** — `YIELD_BY_LINE_5MIN` (the join) and `DEFECT_COUNTS_5MIN`.
   Checkpoint Y. Then show refresh history, and if asked, demonstrate the `MODE()`
   failure as a real teaching moment.

7. **Semantic view + agent — Part 4** — emit both verbatim from `references/`. For the semantic
   view follow its syntax rules exactly (`AS` not `=`, `WITH SYNONYMS`, alias-qualified
   metrics); if a create fails, re-emit the verbatim DDL rather than improvising the
   grammar. For the agent, dollar-quote with `$$` (never a named tag like `$spec$`) and
   set `models.orchestration` to `"auto"`. After creating it, tell the attendee to chat
   with it in **Snowsight → AI & ML → Agents → Cascade Plant Analyst**, on the detail
   page's chat panel — not here. They do NOT need to Publish.

8. **The incident — Part 5** — **do NOT restart the producer.** It has been running since
   step 3 and it stays running for the rest of the lab. Change the *world* instead, by
   writing to the control table:

   ```sql
   INSERT INTO MFG.RAW.SIMULATOR_CONTROL (MODE, UPDATED_AT)
     VALUES ('INCIDENT', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
   ```

   Valid modes are `STEADY`, `INCIDENT` and `REINSPECT`; the newest row wins and the
   producer picks it up within ~10 s. If an attendee asks you to stop or restart the
   producer to change its behaviour, explain that a real Openflow connector runs
   continuously — an incident changes the data at the source, it does not bounce the
   connector — and that restarting also risks HTTP 409
   `ERR_CHANNEL_HAS_UNCOMMITTED_DATA` from reopening a channel too soon.

   Tell the attendee to watch the producer's own log: `booth_humidity` climbs away
   from 44 within seconds, and `[cdc] PAINT defect rate -> 26%` follows ~90 s later.
   That is the cascade, visible before any query. Booth humidity ramps
   first, then PAINT defects spike ~90 s later. Have the attendee stopwatch each layer.
   Then ask the agent *why*. Checkpoint X.

9. **The recovery — Part 5** — again **no restart**. Write `REINSPECT`:

   ```sql
   INSERT INTO MFG.RAW.SIMULATOR_CONTROL (MODE, UPDATED_AT)
     VALUES ('REINSPECT', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
   ```

   This fixes the booth (humidity returns to ~44) and starts a **bounded** burst of
   re-inspections — about 40% of the failed backlog, over ~3 minutes, then back to
   normal cadence on its own. Yield for buckets that already reported goes UP but
   does **not** reach 100%: some frames really are scrap. If yield pins at exactly
   100% with an empty `DEFECT_COUNTS_5MIN`, something is wrong. Inspectors overturn failed frames to
   PASS and **yield goes back up**, including for buckets that already reported.
   Confirm the DTs are still INCREMENTAL. Checkpoint R.

## Checkpoints

- **P (preflight):** `aws_ok`, `cortex_ok`, `raw_iceberg_ok` and `analytics_iceberg_ok` must
  all be TRUE, and every existing Iceberg object must report `is_v3 = TRUE`.
  See `solutions/02_preflight.sql`. Do not proceed past a FALSE.
- **J (journal objects):** the journal reports Iceberg format version 3 and the stream
  reports `mode = APPEND_ONLY`. There should be **no** tasks in the schema:
  ```sql
  SHOW ICEBERG TABLES LIKE 'QUALITY_INSPECTIONS_JOURNAL%' IN SCHEMA MFG.RAW;
  SHOW STREAMS LIKE '%_JOURNAL_%_STREAM' IN SCHEMA MFG.RAW;
  ```
  Do **not** add a "confirm zero tasks exist" check. The no-task fact matters, but it is
  earned in Part 2 by finding the producer's own MERGE in `QUERY_HISTORY` via its
  `QUERY_TAG` — proving a negative in a checkpoint teaches nothing and reads as
  archaeology.
- **I (ingest):** both sources landing.
  ```sql
  SELECT COUNT(*) AS journal_events, COUNT_IF(EVENT_TYPE='IncrementalUpdateRows') AS updates
  FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1;
  SELECT COUNT(*) AS destination_rows FROM MFG.RAW.QUALITY_INSPECTIONS;
  SELECT COUNT(*) AS telemetry_rows,
         DATEDIFF('second', MAX(EVENT_TS), CURRENT_TIMESTAMP()) AS seconds_ago
  FROM MFG.RAW.STATION_TELEMETRY;
  ```
  All climb on re-run. Telemetry `seconds_ago` is ~30 s because of `MAX_CLIENT_LAG` —
  expected, not a fault. The destination trails the journal by up to a minute — also
  expected, and it is the next checkpoint.
- **G-gate (the merge gate):** `journal_inserts` exceeds `destination_rows`, and
  `QUERY_HISTORY` filtered on the connector's `QUERY_TAG` shows one MERGE per minute,
  each starting at second :00 and completing in a second or two. Both queries are in
  `solutions/03_cdc_journal.sql`. The gap is the gate, not the merge.
- **D (layer 1):** `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS` — every row must read
  `refresh_mode = INCREMENTAL`, `is_iceberg = true`, and an empty
  `refresh_mode_reason`.
- **Y (gold):** yield by line for recent buckets. Three lines, each around 95–99% in
  steady state, with `HUMIDITY` populated for PAINT only.
- **X (incident):** PAINT `FIRST_PASS_YIELD_PCT` drops into the 70s while WELD and
  ASSEMBLY stay in the high 90s, `AVG_BOOTH_HUMIDITY` for PAINT climbs from ~44 into
  the 60s–70s, and `PAINT_RUN` dominates the defect counts.
- **R (recovery):** PAINT yield rises again and `COUNT_IF(_SNOWFLAKE_UPDATED_AT >
  _SNOWFLAKE_INSERTED_AT)` on `QUALITY_INSPECTIONS` climbs. The DTs stay INCREMENTAL.

### "Where am I?" — the progress query

If the attendee asks where they are, what they have built, what is missing, whether
anything is flowing, or anything of that shape — **run `solutions/progress.sql`
verbatim**. Do not compose your own version; it is one statement and it is verified.

It lists all eight buildable objects with a built / NOT YET status and an approximate
row count. Reading it for them:

- Everything through their current Part should say `built`.
- `QUALITY_INSPECTIONS` having **fewer** rows than the journal is the merge gate, not a
  fault.
- `YIELD_BY_LINE_5MIN` holding single-digit rows is **correct** — it is three lines
  times the number of elapsed 5-minute buckets. Do not compare it to
  `QUALITY_INSPECTIONS` and report a problem.
- `built` with 0 rows and no growth means the producer is not running, or is running
  without the flag for that feed.

The agent is not in that query — agents have no `INFORMATION_SCHEMA` view. Check it
with `SHOW AGENTS LIKE 'CASCADE_PLANT_ANALYST' IN SCHEMA MFG.ANALYTICS;`.

## Stopping Points

- Do not create any Iceberg table without `USE SCHEMA` on the line before it.
- Do not create any table before the storage defaults are set.
- Do not proceed past Checkpoint P with any FALSE, or any object reporting v2.
  There is no in-place v2 → v3 upgrade; a v2 table must be recreated.
- Confirm the journal and its stream exist before starting the producer in `journal`
  mode — otherwise the events have nowhere to land.
- Never stop or restart the producer to change its behaviour. Write to
  `MFG.RAW.SIMULATOR_CONTROL` instead. Restarting teaches a false operational model and
  risks HTTP 409 from reopening a channel within ~30 s.
- Never create a Snowflake task for the merge. The connector does not, and doing so
  would teach attendees something false about the product.
- Confirm `QUALITY_INSPECTIONS` and `STATION_TELEMETRY` both exist before starting the
  producer.
- Run each layer's Checkpoint before moving to the next.

## References

- `references/object_model.md` — verbatim DDL for the journal, the DAG, and the
  semantic view.
- `references/agent_spec.md` — verbatim agent spec and the three questions.
- `solutions/progress.sql` — the "where am I" query. Run it verbatim.

## Things in this repo that are NOT attendee build steps

Do not offer to generate these, and do not treat a question about them as a build
request. They ship pre-written for stated reasons:

- **`external/read_iceberg.py`** — reads the Gold table from outside Snowflake via
  PyIceberg and the Horizon Catalog. Optional act A. Ships pre-written because the auth
  path has two traps that are not in the PyIceberg docs: a PAT must be **exchanged** for
  an access token first (a PAT as a Bearer token returns 401 with an empty body), and
  PyIceberg's `credential` property is rejected by Horizon — pass `token=` instead. If
  an attendee hits either, point at the comments in that file. They run it with
  `pip install -r external/requirements.txt && python external/read_iceberg.py`.
- **`dashboard/streamlit_app.py`** — the live plant-floor dashboard. **Presenter-only.**
  It is shared on screen during Part 5; it is not a lab step. If an attendee asks, they
  can deploy it after the session with `snow streamlit deploy`.

## Output

The Cascade Cycleworks pipeline: a CDC journal and the producer-issued gated MERGE,
`QUALITY_INSPECTIONS`,
`STATION_TELEMETRY`, four Dynamic Iceberg Tables, `PLANT_FLOOR_SV`, and an agent that
can explain a two-phase quality incident by reading across both sources.
