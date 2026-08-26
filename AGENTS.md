# streaming-cdc-iceberg-vhol

Lab files for the *Build Real-Time Pipelines on Iceberg with AI Agents* virtual
hands-on lab. Setup, the walkthrough, and troubleshooting are in
[README.md](README.md) — start there.

## Working in this repo

- You build the pipeline by **prompting Cortex Code**, not by writing SQL yourself.
  A skill in `.snowflake/cortex/skills/coco-iceberg-cdc-vhol/` loads automatically
  when you open this folder and supplies the exact object names, Iceberg settings,
  and checkpoint queries each part expects.
- **Use the object names the skill gives you.** Later parts and every checkpoint
  query depend on them.
- `solutions/` is the answer key, one file per part. Read it whenever you want — it
  carries inline notes on the constraints that cost time, not just the DDL.
- **Never commit a credential.** The lab mints a token for you. `.gitignore` already
  excludes `secret.pat`, `profile.json`, and key files. Keep it that way — this repo
  is public.

## Two things that break this lab

1. **The three schema defaults.** `EXTERNAL_VOLUME`, `CATALOG` and
   `ICEBERG_VERSION_DEFAULT = 3` must be set on `MFG.CDC` and `MFG.RAW` *before* any
   table is created. `CREATE DYNAMIC ICEBERG TABLE` has no version clause and can
   only inherit them.
2. **`CORTEX_ENABLED_CROSS_REGION`.** Defaults to `DISABLED` on a fresh account,
   which degrades the agent in Part 4. `solutions/00_bootstrap.sql` sets it.

`solutions/02_preflight.sql` checks both. Run it before building anything on top.
