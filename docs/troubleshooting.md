# Troubleshooting

Symptom, cause, fix. Find the section for where you are, then scan the left column.

Back to the walkthrough: [README](../README.md).

## Setup

| Symptom | Cause | Fix |
|---|---|---|
| You did not copy `token_secret` in time | It is shown exactly once | `ALTER USER HOL_USER ROTATE PROGRAMMATIC ACCESS TOKEN HOL_PAT;` in Snowsight returns a fresh one. Do not prompt Cortex Code for it — it is not connected yet. |
| `profile.json is not valid JSON -- line N, column N` | A quote or comma lost while editing the file by hand | Compare against `profile.example.json`; the keys are identical. Only two values change: `account` and `personal_access_token`. |
| `profile.json not found` | Setup B step 3 was skipped | `cp profile.example.json profile.json`, then fill in the account and the token. |
| Producer: authentication fails | Token expired, or `profile.json` has the wrong account | Tokens last 7 days. Re-mint or ROTATE in Snowsight, then paste the new token into `profile.json`. |
| A token is refused even though it was just minted | A programmatic access token only authenticates if its user sits under a network policy | Attach one before minting: `CREATE NETWORK POLICY IF NOT EXISTS HOL_NP ALLOWED_IP_LIST = ('0.0.0.0/0'); ALTER USER HOL_USER SET NETWORK_POLICY = HOL_NP;` — `00_bootstrap.sql` BLOCK 2 does this. |
| Wrong account in `profile.json` | The `cortex` CLI's default connection was read instead of the active one | The account must come from SQL: `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME()`. |
| Dashboard: an edit to `streamlit_app.py` does not appear | A `STREAMLIT` serves an immutable version snapshot; the stage is only its source. `PUT` alone changes nothing the app runs, silently | Re-run the whole deploy so `CREATE OR REPLACE STREAMLIT` re-snapshots. Confirm with `LIST 'snow://streamlit/MFG.ANALYTICS.PLANT_FLOOR_LIVE/versions/version$1/'` — that size and md5 are what is served. Then reload the page. |
| Dashboard: `Gold lag` reads `—` | No successful Dynamic Table refresh is on record yet, or the refresh-history read failed | Normal until Part 3's first refresh completes. If it persists, check `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS` for `scheduling_state = SUSPENDED`. The tiles and chart are unaffected either way. |
| Dashboard: `Gold lag` reads minutes, not seconds | The warehouse is the constraint, or a refresh is failing | It stays under a minute against `TARGET_LAG = '1 minute'`, and read 18 s on a fresh build. Read `refresh_mode` and `refresh_mode_reason`; a table on `FULL` refresh lags far more than one on `INCREMENTAL`. |
| Producer: `externally-managed-environment` | macOS Homebrew Python, PEP 668 | Use the venv interpreter, not system Python. Ask Cortex Code to redo the venv step. |
| `cortex_ok` is FALSE | `CORTEX_ENABLED_CROSS_REGION` still `DISABLED` | Re-run that `ALTER ACCOUNT` from Setup B as ACCOUNTADMIN in Snowsight. |

## Iceberg storage and format version — Part 1

| Symptom | Cause | Fix |
|---|---|---|
| `raw_iceberg_ok` or `analytics_iceberg_ok` is FALSE, or an object reports v2 | Your session's current schema was not one that resolves `ICEBERG_VERSION_DEFAULT = 3` when the table was created | Re-run `01_environment.sql` — it sets the database-level default and issues `USE SCHEMA` before each create. Then **recreate** any v2 table. |
| `SHOW PARAMETERS` says 3 but a table comes out v2 | Not a contradiction. For a plain `CREATE ICEBERG TABLE` the version is applied from the *session's* schema, whatever the target schema reports. Measured on a trial account 26 Aug 2026; undocumented, and the docs imply symmetric inheritance, so it may change | `USE SCHEMA MFG.RAW;` immediately before the `CREATE`. Never trust `SHOW PARAMETERS` as proof — only a created table's `iceberg_table_format_version` counts. |
| `Unsupported data type 'VARIANT' for iceberg tables` | Same cause — the table resolved to v2 | Same fix. This is the error the journal throws, since `SF_METADATA` is `VARIANT`. |
| Rejected `TIMESTAMP_NTZ(9)` from `TIME_SLICE()` | The Dynamic Table landed on v2, because `MFG.ANALYTICS`'s own `ICEBERG_VERSION_DEFAULT` was not 3 when it was created — a Dynamic Table takes its version from the target schema | Set the three defaults on `MFG.ANALYTICS` (`01_environment.sql` does), confirm with `analytics_iceberg_ok`, then recreate the Dynamic Table. |
| A column `DEFAULT` clause is rejected | `DEFAULT` is unavailable on v2 and gated on v3 | The producer supplies timestamps instead. Do not add a `DEFAULT` to the landing tables. |

## Ingestion — Parts 1 and 2

| Symptom | Cause | Fix |
|---|---|---|
| Destination table stays behind the journal | That is the merge gate, by design | Check `QUERY_HISTORY` for the connector's `QUERY_TAG`. Merges fire at second :00 each minute. Nothing to fix. |
| Destination table gets **no** rows at all | The producer was started with `--no-merge`, or the journal objects do not exist | Restart the producer without `--no-merge`, and confirm the journal and its stream exist. |
| `SF_METADATA:offset_token` returns NULL | It holds a JSON string, not an object — faithful connector behaviour | `PARSE_JSON(SF_METADATA::STRING):offset_token` |
| Telemetry rows take ~30 s to appear | Normal flush behaviour for a streaming Iceberg target | Expected behaviour, not a fault. |
| No `CREATE PIPE` anywhere, and you are looking for the pipe | Snowpipe Streaming auto-creates a default pipe for the table | Expected. Look for `STATION_TELEMETRY-STREAMING`. You never write `CREATE PIPE`. |
| Producer: `ERR_CHANNEL_HAS_UNCOMMITTED_DATA` (HTTP 409) | You stopped the producer and started it again within ~30 s, reopening a channel that was still committing | The lab never asks you to restart it — Part 5 changes modes through the control table instead. If you did stop it, wait ~30 s. Never run two producers at once. |

## Dynamic Tables — Part 3

| Symptom | Cause | Fix |
|---|---|---|
| `refresh_mode` comes back `FULL` | Something in the query blocks incremental refresh | Read `refresh_mode_reason`; it names the cause. `APPROX_PERCENTILE` is a common one. |
| `Change tracking is not supported ... 'MODE'` | `MODE()` in a Dynamic Table | Count at defect grain and rank at read time. |
| The contract check reports `-- MISSING COLUMN --` | The table was built with a different name for that column | Re-run the Part 3 creation prompt for **that table only**, naming the column it flagged. A Dynamic Table's columns come from its query, so do not try to `ALTER` the name. |
| The contract check reports `-- WRONG TYPE --` on `IS_SCRAP` | It was derived as a `BOOLEAN` | It must be `IFF(STATUS = 'FAIL', 1, 0)`. The Gold layer takes `SUM(IS_SCRAP)`, which a `BOOLEAN` breaks. |
| `GRANT SELECT ON ALL TABLES` leaves the Dynamic Tables unreadable | That grant does not include Dynamic Tables, and reports no error | Grant them explicitly: `ON ALL DYNAMIC TABLES` and `ON FUTURE DYNAMIC TABLES`. |
| A Dynamic Table with `OBJECT` or `OBJECT_AGG` output will not create | Those types cannot land in an Iceberg table, on v2 or v3 | Return the fields as columns instead of an object. |
| The pipeline refreshes more slowly than the 1-minute lag you set | A layer was created with `TARGET_LAG = DOWNSTREAM`, which inherits from its consumer | Pin `TARGET_LAG = '1 minute'` on every layer. `SHOW DYNAMIC TABLES` reports each one's `target_lag`. |
| A Dynamic Table using `APPROX_PERCENTILE` refreshes `FULL` | That function is not incremental | Remove it. `refresh_mode_reason` names it. |

## Semantic view — Part 4

| Symptom | Cause | Fix |
|---|---|---|
| `CREATE SEMANTIC VIEW` fails on syntax | One of four forms is easy to get wrong | Clause order is fixed: `TABLES` → `RELATIONSHIPS` → `FACTS` → `DIMENSIONS` → `METRICS`. Bind tables with `AS`, not `=`. Use `WITH SYNONYMS = (...)`, not a bare `SYNONYMS = (...)`. Define metrics alias-qualified with `AS`: `yield.total_units AS SUM(yield.units)`. Re-emit `05_semantic_view.sql` verbatim rather than improvising. |

## Agent — Parts 4 and 5

| Symptom | Cause | Fix |
|---|---|---|
| Agent: `internal error (request_id: …)`, code 391920 | The Analyst tool has no `execution_environment`, so its generated SQL has no warehouse to run in | Add `"execution_environment": { "type": "warehouse", "warehouse": "HOL_WH" }` to the `tool_resources` entry and re-run `CREATE OR REPLACE AGENT`. |
| Agent errors before it answers anything, and `execution_environment` is present | The user calling the agent has no default warehouse. An agent resolves the default role *and* the default warehouse from the user, not from the session | `ALTER USER <you> SET DEFAULT_WAREHOUSE = HOL_WH;` — including the Snowsight user you are chatting as, which on a trial is the signup admin. |
| `CREATE AGENT` fails on `unexpected '$spec'` | The specification was dollar-quoted with a named tag | Use bare `$$`. The spec JSON never contains `$$` itself, so it is safe. |
| Agent: *"not an allowed model for Agent"* | A specific orchestration model was pinned | Use `"orchestration": "auto"`. Agent orchestration has a narrower allowed-models list than Cortex `COMPLETE`. Pin a model later in Snowsight under **Configuration → Model** if you want one. |
| Agent errors or lists no models | Cross-region inference disabled | See `cortex_ok` above. |
| An edit to the agent does not take effect, or fails on `Could not resolve workspace file ... cortex-project.yaml` | This agent is created from SQL and is not tracked in a workspace | Change it by re-running the whole `CREATE OR REPLACE AGENT` statement. |
| You cannot find where to chat with it | The chat panel is on the agent's detail page | **Snowsight → AI & ML → Agents → Cascade Plant Analyst**. You do not need to Publish; the agent already exists from the SQL. Publish is only for sharing a UI-edited version. |
| Agent answers with stale numbers | The pipeline lags 1–2 min by design | Ask again in a minute. "Right now" means the most recent complete buckets. |
| Agent answers *"the top defect is NONE"* | It is not excluding passed scans. `DEFECT_CODE = 'NONE'` means the scan passed, and it is the most common value in the table | Add the exclusion to the agent's orchestration instructions and re-run `CREATE OR REPLACE AGENT`. |
| Agent names the right cause but the sequence backwards | It found the correlation without the ordering | Ask it which came first. Humidity climbs before defects follow. |
| A number changed since you last asked the same question | Correct, and interesting — inspectors overturn failed frames, which rewrites buckets already reported | Nothing to fix. That is Part 5's recovery. |
| Part 5: `SIMULATOR_CONTROL does not exist`, or the producer logs `[control] read failed` | Part 1 created the landing tables but not the control table | Run [`solutions/01_environment.sql`](../solutions/01_environment.sql), which creates all three. Then re-run the Part 5 prompt. |

## Dashboard — Part 5 (optional)

| Symptom | Cause | Fix |
|---|---|---|
| The app loads blank, or errors before any chart | The source file was gzipped on upload. `PUT` compresses by default | Re-upload with `AUTO_COMPRESS = FALSE OVERWRITE = TRUE`, then `CREATE OR REPLACE STREAMLIT` again. |
| *"No data yet — the pipeline has not produced any 5-minute buckets"*, and the producer **is** running | The Dynamic Tables are suspended, so rows land in `QUALITY_INSPECTIONS` while the layers above it stay frozen. Cleanup Block 1 suspends them | `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS` — any `scheduling_state = SUSPENDED` needs `ALTER DYNAMIC TABLE <name> RESUME`, upstream first. Give it a minute per layer. |
| *"No data yet — the pipeline has not produced any 5-minute buckets"* | The producer is not running, or no 5-minute bucket has closed yet | Start the producer and wait about a minute. Confirm with the progress query. |
| `Query failed: invalid identifier 'BUCKET'` — or `LINE`, `N`, `UNITS` | A Dynamic Table came out with a different column name | Run the column contract. It names the table and column; re-run that table's Part 3 prompt naming the column. |
| Yield chart is empty but the tiles show numbers | The newest bucket is older than the chart's 60-minute window | The producer stopped. Restart it; earlier buckets stay as they were. |
| Defect panel says *"No defects recorded in the last 15 minutes"* | Correct in steady state, and correct again once the producer has been idle 15 minutes | Nothing to fix. It fills in during the incident. |
| `TypeError: unsupported operand` or a chart axis of `Decimal` values | The `::FLOAT` cast on `FIRST_PASS_YIELD_PCT` was removed | Put it back. The connector maps any `NUMBER` with scale > 0 to `decimal.Decimal`, which Altair cannot plot. |
| `AttributeError: module 'streamlit' has no attribute 'fragment'`, or `TypeError: container() got an unexpected keyword argument 'horizontal'` | The `environment.yml` pin did not reach the app, so it resolved an older Streamlit. The app needs `st.fragment` (1.37+) and horizontal containers (1.49+) | `LIST @MFG.ANALYTICS.DASHBOARD_STAGE/plant_floor` must show `environment.yml` beside `streamlit_app.py`. `PUT` it if missing, then `CREATE OR REPLACE STREAMLIT`. Check the account offers the pinned version: `SELECT VERSION, RUNTIME_VERSION FROM INFORMATION_SCHEMA.PACKAGES WHERE PACKAGE_NAME = 'streamlit'`. |
| You changed `streamlit_app.py`, re-`PUT` it, and the app still runs the old code | `PUT` updates the stage, and the running app does not pick it up on a browser reload alone | Re-run `CREATE OR REPLACE STREAMLIT ...` after the `PUT`, then reload. Restarting the app from Snowsight may be enough on its own; recreating the object definitely is. |
| You cannot find the app | It is not under Worksheets | **Snowsight → Projects → Streamlit → Plant Floor — Live Quality**. |

## External read — Part 6

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: No module named 'pyiceberg'` | The script ran on system Python, or Setup D installed only the producer's requirements | Run it with the venv interpreter, `.venv/bin/python external/read_iceberg.py`. If the import still fails, `.venv/bin/pip install -r external/requirements.txt`. |
| 401 with an empty body | A PAT presented directly as a Bearer token. It must be exchanged for an access token first | `external/read_iceberg.py` does the exchange. If you wrote your own, see the comments in it. |
| `OAuthError: unauthorized_client` | PyIceberg's `credential` property formats the request in a way Horizon rejects | Pass `token=<access_token>` instead. |
| HTTP 404 on the catalog | Catalog or namespace name is lower-case | Uppercase them. `warehouse=` is the **database** name, uppercase. |

## Snowsight

| Symptom | Cause | Fix |
|---|---|---|
| `SQL compilation error: Empty SQL statement` at the end of a `solutions/` file | Snowsight parses whatever follows the last statement as a statement, so a file ending in comments errors | Harmless — everything above it ran. Every file ends with a `SELECT '… complete'` so you get a confirmation row instead. If you see it, check the statements above succeeded. |

---
