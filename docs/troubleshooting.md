# Troubleshooting

Symptom, cause, fix. Scan the left column for what you are seeing.

Back to the walkthrough: [README](../README.md).

| Symptom | Cause | Fix |
|---|---|---|
| `raw_iceberg_ok` or `analytics_iceberg_ok` is FALSE, or an object reports v2 | Your session's current schema was not one that resolves `ICEBERG_VERSION_DEFAULT = 3` when the table was created | Re-run `01_environment.sql` — it sets the database-level default and issues `USE SCHEMA` before each create. Then **recreate** any v2 table. |
| `Unsupported data type 'VARIANT' for iceberg tables` | Same cause — the table resolved to v2 | Same fix. This is the error the journal throws, since `SF_METADATA` is `VARIANT`. |
| `SHOW PARAMETERS` says 3 but a table comes out v2 | Not a contradiction. For a plain `CREATE ICEBERG TABLE` the version is applied from the *session's* schema, whatever the target schema reports | `USE SCHEMA MFG.RAW;` immediately before the `CREATE`. Never trust `SHOW PARAMETERS` as proof — only a created table's `iceberg_table_format_version` counts. |
| `cortex_ok` is FALSE | `CORTEX_ENABLED_CROSS_REGION` still `DISABLED` | Re-run that `ALTER ACCOUNT` from Setup B as ACCOUNTADMIN in Snowsight. |
| Rejected `TIMESTAMP_NTZ(9)` from `TIME_SLICE()` | The Dynamic Table landed on v2, because `MFG.ANALYTICS`'s own `ICEBERG_VERSION_DEFAULT` was not 3 when it was created — a Dynamic Table reads the target schema and has no version clause to override with | Set the three defaults on `MFG.ANALYTICS` (`01_environment.sql` does), confirm with `analytics_iceberg_ok`, then recreate the Dynamic Table. |
| `refresh_mode` comes back `FULL` | Something in the query blocks incremental refresh | Read `refresh_mode_reason`; it names the cause. `APPROX_PERCENTILE` is a common one. |
| `Change tracking is not supported ... 'MODE'` | `MODE()` in a Dynamic Table | Expected — that is Optional B. Count at defect grain, rank at read time. |
| Destination table stays behind the journal | That is the merge gate, by design | Check `QUERY_HISTORY` for the connector's `QUERY_TAG`. Merges fire at second :00 each minute. Nothing to fix. |
| Destination table gets **no** rows at all | The producer was started with `--no-merge`, or the journal objects do not exist | Restart the producer without `--no-merge`, and confirm the journal and its stream exist. |
| `SF_METADATA:offset_token` returns NULL | It holds a JSON string, not an object — faithful connector behaviour | `PARSE_JSON(SF_METADATA::STRING):offset_token` |
| Telemetry rows take ~30 s to appear | Normal flush behaviour for a streaming Iceberg target | Expected behaviour, not a fault. |
| Producer: `ERR_CHANNEL_HAS_UNCOMMITTED_DATA` (HTTP 409) | You stopped the producer and started it again within ~30 s, reopening a channel that was still committing | The lab never asks you to restart it — Part 5 changes modes through the control table instead. If you did stop it, wait ~30 s. Never run two producers at once. |
| Producer: `externally-managed-environment` | macOS Homebrew Python, PEP 668 | Use the venv interpreter, not system Python. Ask Cortex Code to redo the venv step. |
| Snowsight: `SQL compilation error: Empty SQL statement` at the end of a `solutions/` file | Snowsight parses whatever follows the last statement as a statement, so a file ending in comments errors | Harmless — everything above it ran. Every file now ends with a `SELECT '… complete'` so you get a confirmation row instead. If you see it, check the statements above succeeded. |
| You did not copy `token_secret` in time | It is shown exactly once | `ALTER USER HOL_USER ROTATE PROGRAMMATIC ACCESS TOKEN HOL_PAT;` in Snowsight returns a fresh one. Do not prompt Cortex Code for it — it is not connected yet. |
| Part 5: `SIMULATOR_CONTROL does not exist`, or the producer logs `[control] read failed` | Part 1 created the landing tables but not the control table | Run [`solutions/01_environment.sql`](../solutions/01_environment.sql), which creates all three. Then re-run the Part 5 prompt. |
| Producer: authentication fails | Token expired, or `profile.json` has the wrong account | Tokens last 7 days. Re-mint in Snowsight and rebuild `profile.json`. |
| Agent: `internal error (request_id: …)`, code 391920 | The Analyst tool has no `execution_environment`, so its generated SQL has no warehouse to run in | Add `"execution_environment": { "type": "warehouse", "warehouse": "HOL_WH" }` to the `tool_resources` entry and re-run `CREATE OR REPLACE AGENT`. |
| Agent answers with stale numbers | The pipeline lags 1–2 min by design | Ask again in a minute. "Right now" means the most recent complete buckets. |
| Agent errors or lists no models | Cross-region inference disabled | See `cortex_ok` above. |
| Agent: *"not an allowed model for Agent"* | A specific orchestration model was pinned | Use `"orchestration": "auto"`. Agent orchestration has a narrower allowed-models list than Cortex `COMPLETE`. |
| Agent answers *"the top defect is NONE"* | It is not excluding passed scans. `DEFECT_CODE = 'NONE'` means the scan passed, and it is the most common value in the table | Add the exclusion to the agent's orchestration instructions and re-run `CREATE OR REPLACE AGENT`. |
| Agent names the right cause but the sequence backwards | It found the correlation without the ordering | Ask it which came first. Humidity climbs before defects follow. |
| A number changed since you last asked the same question | Correct, and interesting — inspectors overturn failed frames, which rewrites buckets already reported | Nothing to fix. That is Part 5's recovery. |
| Part 6: `ModuleNotFoundError: No module named 'pyiceberg'` | The script ran on system Python, or Setup D installed only the producer's requirements | Run it with the venv interpreter, `.venv/bin/python external/read_iceberg.py`. If the import still fails, `.venv/bin/pip install -r external/requirements.txt`. |
| External read: 401 with an empty body | A PAT presented directly as a Bearer token. It must be exchanged for an access token first | `external/read_iceberg.py` does the exchange. If you wrote your own, see the comments in it. |
| External read: `OAuthError: unauthorized_client` | PyIceberg's `credential` property formats the request in a way Horizon rejects | Pass `token=<access_token>` instead. |
| External read: HTTP 404 on the catalog | Catalog or namespace name is lower-case | Uppercase them. `warehouse=` is the **database** name, uppercase. |
| Wrong account shows up in `profile.json` | The `cortex` CLI's default connection was used instead of the active one | The account must come from SQL: `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME()`. |

---
