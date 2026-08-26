---
name: iceberg-external-read
description: "Reads this lab's Snowflake-managed Apache Iceberg tables from outside Snowflake, using PyIceberg against the Horizon REST catalog with vended credentials — no warehouse computes the scan. This is Part 6 of the streaming CDC on Iceberg lab, the closing act, and it needs nothing from the core build beyond a Gold table existing. Carries the two auth traps that are absent from the PyIceberg docs, and the four failure modes attendees actually hit. Use when: reading Iceberg tables from a laptop or from outside Snowflake; running or debugging external/read_iceberg.py; proving data is open rather than locked in; anything involving PyIceberg, the Horizon catalog, vended credentials, or predicate pushdown from an external engine; or any 401, OAuthError unauthorized_client, or 404 from the catalog. Triggers: part 6, optional A, read_iceberg.py, pyiceberg, horizon catalog, horizon IRC, external read, read from my laptop, read outside snowflake, open table format proof, vended credentials, load_catalog, predicate pushdown, unauthorized_client, PAT exchange, access token exchange, catalog 404, s3 sfc customer-interop."
---

# Part 6 — read the Iceberg tables from outside Snowflake

The closing claim of this lab is that the data is in **open** Iceberg — governed by
Snowflake, not locked inside it. This Part proves it: PyIceberg reads the Gold Dynamic
Table straight from object storage through the Horizon REST catalog, using vended
credentials. No Snowflake warehouse computes the scan.

**This is the last Part of the core lab (D36), and it is self-contained.** It needs only
that a Gold Dynamic Table exists, it does not touch the pipeline, and nothing later
depends on it. So it is safe to run at any point after Part 3, and a failure here costs
the attendee nothing they built — say so, so they do not think the pipeline broke.

## Ships pre-written — do not generate it

`external/read_iceberg.py` is **pre-written on purpose** (D19, unchanged by D36). Do not
offer to write it, and do not rewrite it if it fails. The auth path has two steps that are
absent from the PyIceberg documentation, so a generated draft is broken in ways that cost
more time than the Part is worth. Point the attendee at the script's own comments.

How they run it:

```bash
.venv/bin/python external/read_iceberg.py
```

PyIceberg is already in the venv: Setup D installs `external/requirements.txt` alongside
the producer's, so nothing installs during the session. If the import fails, install it
into the venv rather than system-wide —
`.venv/bin/pip install -r external/requirements.txt`. On Windows the interpreter is
`.venv\Scripts\python.exe`.

## The two traps that are not in the PyIceberg docs

1. **A PAT must be exchanged for an access token first.** Presenting the PAT directly as
   a Bearer token returns **401 with an empty body** — no message, nothing to search for.
   `read_iceberg.py` does the exchange.
2. **PyIceberg's `credential` property is rejected by Horizon.** It formats the token
   request in a way Horizon does not accept, failing with
   `OAuthError: unauthorized_client`. Pass `token=<access_token>` instead.

## Checkpoint

The script prints, in order:

- Iceberg format version **`v3`**
- a storage path under Snowflake's managed bucket — `s3://sfc-…-customer-interop-fs-…`
- the rows
- a **smaller** row count after predicate pushdown on `LINE == 'PAINT'`

That storage path is what proves the claim: those are the same bytes Snowflake reads,
read by an engine that has never heard of Snowflake.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| 401 with an empty body | A PAT was presented directly as a Bearer token | It must be exchanged for an access token first. `read_iceberg.py` does this; if they wrote their own, point at its comments. |
| `OAuthError: unauthorized_client` | PyIceberg's `credential` property formats the request in a way Horizon rejects | Pass `token=<access_token>` instead. |
| HTTP 404 on the catalog | Catalog or namespace name is lower-case | Uppercase them. `warehouse=` is the **database** name, also uppercase. |
| Reads nothing, but no error | No Gold Dynamic Table exists yet, or it is suspended | This act needs `MFG.ANALYTICS.YIELD_BY_LINE_5MIN` to exist. Check with `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS`. |

## Billing

Horizon catalog access is billed as **external-engine access**, even when the reader is
another Snowflake account. That does not change the architectural claim, since the data is
open and readable by any Iceberg engine, but it is not a free read, and an attendee
planning to copy this pattern should know.

## Stopping points

- Do not generate or rewrite `external/read_iceberg.py`. It ships working.
- Do not create, alter or drop any pipeline object here. This act is read-only.
- Do not treat a failure as a lab failure. It is optional and it stands alone.
