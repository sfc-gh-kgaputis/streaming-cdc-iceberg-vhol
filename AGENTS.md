# streaming-cdc-iceberg-vhol

Lab files for the *Build Real-Time Pipelines on Iceberg with AI Agents* virtual hands-on lab.
Setup and prerequisites are in [README.md](README.md).

**Lab content is not committed yet.** Only the prerequisites are here so far.

## Working in this repo

- You build the pipeline by **prompting CoCo**, not by writing SQL yourself. Once the lab content
  lands, a skill in `.snowflake/cortex/skills/` loads automatically and supplies the exact object
  names and settings each step expects.
- **Use the object names the skill gives you.** Later parts of the lab, and the checkpoint queries,
  depend on them.
- **Never commit a credential.** The lab creates a token for you; `.gitignore` already excludes
  `secret.pat`, `profile.json`, and key files. Keep it that way — this repo is public.
