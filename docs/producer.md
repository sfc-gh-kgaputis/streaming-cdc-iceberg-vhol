# The producer

Reference for `producer/main.py`. You never need to edit it, and the lab only ever asks you to run the
one command in [Part 2](../README.md#part-2--watch-the-connectors-change-feed).

You never need to edit it. Run it with the venv interpreter so it finds the SDK:
`.venv/bin/python` on macOS/Linux, `.venv\Scripts\python.exe` on Windows.

You start it **once**, in Part 2, with both sources:

```bash
.venv/bin/python producer/main.py --profile producer/profile.json --cdc --telemetry
```

That is the only command the lab asks you to run. Part 5's incident and recovery are triggered by
writing to `MFG.RAW.SIMULATOR_CONTROL` while it keeps streaming.

```bash
# see what it generates, no Snowflake account needed
.venv/bin/python producer/main.py --dry-run --cdc --seed 42
```

`--incident` and `--reinspect` also exist as startup flags, and `--no-control` ignores the control table
entirely. They are for rehearsing from a shell, and they require stopping the producer. You do not need
them.

`--rate` sets scans/sec (default 2), `--telemetry-rate` sets telemetry rows/sec (default 60).
`--seed` makes a run reproducible. `--help` lists the rest.

`--cdc-mode` picks how the CDC half writes:

- `journal` (default). Change events go to the journal over Snowpipe Streaming, and the producer issues
  the MERGE on its CRON gate. This is the path the lab uses.
- `direct`. Writes the settled result straight to `QUALITY_INSPECTIONS` with ordinary DML: no journal, no
  stream, no merge gate. Use it only if the journal objects are missing and you need rows flowing.
