# streaming-cdc-iceberg-vhol

Lab files for the *Build Real-Time Pipelines on Iceberg with AI Agents* virtual
hands-on lab. Setup, the walkthrough, and troubleshooting are in
[README.md](README.md) — start there.

## Working in this repo

- **The subject is the technology, not the bicycles.** Cascade Cycleworks is a worked
  example. What you are learning is real-time ingestion into open Iceberg, incremental
  transformation, serving to an agent, and how to drive Cortex Code — all of which
  transfer to any source that mutates and any metric someone needs sooner. When you
  explain something, name the capability first and the plant second.
- You build the pipeline by **prompting Cortex Code**, not by writing SQL yourself.
  Two skills in `.snowflake/cortex/skills/` load automatically when you open this
  folder. Nothing to install. You can name either explicitly with `$`, but you
  never have to.
- **Use the object names the skill gives you.** Later parts and every checkpoint
  query depend on them.
- `solutions/` is the answer key, one file per part. Read it whenever you want — it
  explains why each object is shaped the way it is, not just the DDL. When something
  goes wrong, go to [docs/troubleshooting.md](docs/troubleshooting.md) instead.
- **Never commit a credential.** The lab mints a token for you. `.gitignore` already
  excludes `secret.pat`, `profile.json`, and key files. Keep it that way — this repo
  is public.

## Two things to set before you build

1. **The three schema defaults.** Set `EXTERNAL_VOLUME`, `CATALOG` and
   `ICEBERG_VERSION_DEFAULT = 3` on `MFG.RAW` and `MFG.ANALYTICS` both, *before* any
   table is created. `CREATE DYNAMIC ICEBERG TABLE` has no version clause and can
   only inherit them.
2. **`CORTEX_ENABLED_CROSS_REGION`.** Set it to `ANY_REGION`, which
   `solutions/00_bootstrap.sql` does. A fresh account defaults to `DISABLED`, which
   degrades the agent in Part 4.

`solutions/02_preflight.sql` checks both. Run it before building anything on top.
