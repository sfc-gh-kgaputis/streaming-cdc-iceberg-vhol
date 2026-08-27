# Build Real-Time Pipelines on Iceberg with AI Agents

Virtual Hands-On Lab · **27 August 2026, 10:00 AM PT** ·
[Register](https://www.snowflake.com/en/webinars/virtual-hands-on-lab/build-realtime-pipelines-on-iceberg-with-ai-agents-2026-08-27/)

You will build a real-time manufacturing pipeline on Snowflake. Change data capture from an
operational database lands in **Apache Iceberg** tables, **Dynamic Tables** refine it continuously,
and an **AI agent** explains what is happening on the plant floor. You build it by prompting
**Cortex Code**, not by pasting SQL.

You leave with one open lakehouse: governed by Snowflake, readable by any engine that speaks Iceberg.

![Two feeds land in Snowflake-managed Apache Iceberg v3 tables, both over Snowpipe Streaming. Station telemetry streams straight into the STATION_TELEMETRY Iceberg table. In parallel, a simulated Openflow Postgres CDC connector appends change events to the QUALITY_INSPECTIONS_JOURNAL Iceberg table, and an APPEND_ONLY stream on that journal feeds a MERGE the connector issues itself on a one-minute gate, maintaining the QUALITY_INSPECTIONS destination table with soft deletes. Four Dynamic Iceberg Tables refine both feeds incrementally on a one-minute target lag: STATION_HEALTH rolls up telemetry and INSPECTIONS_ACTIVE filters soft-deleted rows, then YIELD_BY_LINE_5MIN joins those two on line and five-minute bucket while DEFECT_COUNTS_5MIN counts defects at their natural grain. The semantic view PLANT_FLOOR_SV sits over three tables — YIELD_BY_LINE_5MIN, DEFECT_COUNTS_5MIN and STATION_HEALTH, which reaches the view directly as well as through the join — and a Cortex Agent answers questions through that view. Outside Snowflake, PyIceberg on your laptop reads the same gold Iceberg tables through the Horizon Catalog with vended credentials, using no warehouse.](docs/architecture.svg)

## The scenario

**Cascade Cycleworks** makes bicycle frames. Three lines run in sequence,
**WELD → PAINT → ASSEMBLY**, and every frame is scanned at the end of each line as PASS or FAIL with
a defect code. That inspection data lives in the plant's MES on Postgres. Separately, sensors on each
station stream temperature, humidity, current and torque readings.

Today yield is reported at end of shift, by which point a bad run has eaten a shift of material. The
target is **two minutes**, because the decision it feeds is someone walking to the paint booth to stop
the line. Match the latency to the loop it serves: sub-second is wasted here, end-of-shift is useless.

## What you will understand by the end

Nothing below is specific to manufacturing. It applies wherever an operational source mutates and
somebody needs a metric sooner than they are getting it.

- **Two low-latency paths land operational data in an open table format.** A managed CDC connector
  replicates a database including its **updates and deletes**; a
  [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview)
  client writes high-volume telemetry beside it. Both target Apache Iceberg on Snowflake-managed storage,
  so there is no bucket to provision and no IAM role to write.
- **[Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about) are declarative
  incremental transformation.** State the query and a target lag; Snowflake recomputes only what changed.
  No orchestrator, no schedule, no task. Here: four layered Dynamic Iceberg Tables that stay
  `INCREMENTAL` while the tables underneath them are rewritten continuously.
- **A late-arriving correction restates an aggregate that already reported.** One wrong predicate
  silently corrupts a metric for ever; the right one lets history be revised. An append-only pipeline
  cannot do this. Here: yield rises for a 5-minute bucket that had already closed.
- **A [semantic view](https://docs.snowflake.com/en/user-guide/views-semantic/overview) makes a table
  answerable in plain language, and a second source makes the answer causal.** One feed tells you *what*
  happened, never *why*. Here: the agent ties a sensor drift to a defect spike and gets the order right.
- **The output is open Apache Iceberg, not a Snowflake format.** Any engine that speaks Iceberg reads the
  same bytes, with no Snowflake compute in the path. Here: PyIceberg on your laptop.
- **You build it by prompting rather than pasting SQL.** A prompt that names the object, the grain and
  the logic gets you the right table. Here: four Dynamic Tables from two prompts.

## What is real, and what is simulated

**Only the CDC connector is simulated.** A Python script stands in for Openflow's Postgres CDC
connector, because standing up a source database and a connector runtime is an infrastructure
exercise rather than a data-engineering one. Everything downstream is what you would build for real:
the Iceberg tables, the journal, the append-only stream, the MERGE, the Dynamic Tables, the semantic
view, the agent.

It is faithful where it counts: it creates its own destination table, journal and stream, writes the
connector's journal schema and soft deletes, and issues the MERGE on the connector's CRON gate with no
Snowflake task.

**Storage is [Snowflake-managed Iceberg](https://docs.snowflake.com/en/user-guide/tables-iceberg-storage)**
(`EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'`). Snowflake owns the bucket, the catalog and file maintenance:
no cloud storage to provision, no IAM role to write, and the tables are still open Iceberg. Use format
version **3** — the journal's `SF_METADATA` column is a `VARIANT`, which v2 rejects.

## What you need

Everything here is **pre-work**. Nothing installs during the session.

- A Snowflake trial account, created with the signup link provided for this event. You will be
  ACCOUNTADMIN.
- **Cortex Code Desktop**, the desktop application specifically
  ([download](https://www.snowflake.com/en/product/snowflake-coco/downloads/)). Use Desktop, not the
  CLI and not the Snowsight version: the CLI is not offered on standard trial accounts, and Snowsight
  has no local shell, so it cannot create a virtual environment or run the producer.
- **Snowsight** in a browser tab, logged in to the same account. You will switch to it twice.
- **Git** and **Python 3.10–3.13** locally. (3.14 works on macOS and Linux if you have a compiler;
  PyIceberg has no 3.14 wheel yet and builds from source, which can fail on Windows.)

### Repo layout

| | |
|---|---|
| `producer/` | The data producer. You start it once, in Part 2, and leave it running. `main.py` is the file you invoke; `cdc_simulator.py` is the simulated connector, and is worth reading. |
| `solutions/` | The fast path: finished SQL for every Part, numbered to match. Safe to run at any time before Part 2; once the producer is streaming, run only the file for the Part you are on. Plus `progress.sql` ("where am I?") and `09_cleanup.sql`. |
| `external/` | Part 6: read your Iceberg tables from your laptop with PyIceberg. |
| `docs/` | [Troubleshooting](docs/troubleshooting.md), [CDC internals](docs/cdc-internals.md), [producer reference](docs/producer.md), the architecture diagram, and the three agent questions. |
| `dashboard/` | The live dashboard the presenter shares on screen. Not a lab step. |
| `.snowflake/` | Two Cortex Code skills that load automatically. See [How the skills work](#how-the-skills-work). |

## Contents

| | |
|---|---|
| [Setup A–D](#setup--do-this-before-the-session) | Pre-work: repo, account bootstrap, connection, local environment |
| [Part 1](#part-1--land-both-feeds-in-iceberg) | Create the Iceberg targets and the tables you own |
| [Part 2](#part-2--watch-the-connectors-change-feed) | Start the producer; read the CDC journal and the merge gate |
| [Part 3](#part-3--refine-it-with-dynamic-tables) | Four Dynamic Iceberg Tables over both feeds |
| [Part 4](#part-4--ask-it-questions-in-english) | Semantic view, then a Cortex Agent over it |
| [Part 5](#part-5--the-incident-and-the-recovery) | An incident, then a correction that rewrites history |
| [Part 6](#part-6--read-it-from-your-laptop) | Read the same tables with PyIceberg, no warehouse |
| [Optional A](#optional-a--look-inside-the-connector) · [B](#optional-b--break-it-on-purpose) · [C](#optional-c--price-the-predicate) | Connector internals; a deliberate failure; the cost of one predicate |
| [Troubleshooting](docs/troubleshooting.md) | Symptoms, causes and fixes, grouped by Part |
| [Cleanup](#cleanup) | Stop the spend. Do not skip it. |

---

# Setup — do this before the session

Do these in order.

## A. Get the lab files

1. **Clone the repo** and open the folder in Cortex Code Desktop:

   ```bash
   git clone https://github.com/sfc-gh-kgaputis/streaming-cdc-iceberg-vhol.git
   cd streaming-cdc-iceberg-vhol
   ```

   When Cortex Code asks whether you trust the folder, choose **Trust**. An untrusted folder runs in
   restricted mode, and the two skills this lab ships do not load.

## B. Bootstrap the account (Snowsight)

2. **Log in to Snowsight** as your signup ACCOUNTADMIN, open a worksheet, and run the SQL below. It is
   also in [`solutions/00_bootstrap.sql`](solutions/00_bootstrap.sql). This one block is SQL you paste
   rather than a prompt, because Cortex Code cannot create the login it is about to connect as.

   ```sql
   USE ROLE ACCOUNTADMIN;

   -- The producer emits UTC. Set the account to UTC so every latency measurement
   -- below reads correctly.
   ALTER ACCOUNT SET TIMEZONE = 'UTC';

   -- REQUIRED for the agent in Part 4. Set it now: a fresh account defaults to
   -- DISABLED, which shrinks the available models and Cortex features.
   ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

   -- One identity for both Cortex Code and the producer, so you manage one credential.
   CREATE USER IF NOT EXISTS HOL_USER
     DEFAULT_ROLE = ACCOUNTADMIN
     COMMENT = 'Iceberg CDC VHOL lab user';
   GRANT ROLE ACCOUNTADMIN TO USER HOL_USER;

   -- Grant Cortex access explicitly. ACCOUNTADMIN does not imply it, and the
   -- agent step needs it.
   GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER  TO ROLE ACCOUNTADMIN;
   GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER TO ROLE ACCOUNTADMIN;

   -- Attach a network policy before minting the token. A token only authenticates
   -- if its user sits under one. This one is wide open because the account is a
   -- throwaway lab account; do not copy it into anything real.
   CREATE NETWORK POLICY IF NOT EXISTS HOL_NP ALLOWED_IP_LIST = ('0.0.0.0/0');
   ALTER USER HOL_USER SET NETWORK_POLICY = HOL_NP;

   ALTER USER HOL_USER
     ADD PROGRAMMATIC ACCESS TOKEN HOL_PAT
       ROLE_RESTRICTION = 'ACCOUNTADMIN'
       DAYS_TO_EXPIRY = 7
       COMMENT = 'Iceberg CDC VHOL lab token';
   ```

3. **Copy the `token_secret` value now. It is shown once.** Create a file called `secret.pat` in the
   root of this repo and paste the token into it. It is gitignored.

   Missed it? The token is unrecoverable, but you do not need to start over. Rotate it to get a fresh
   one, in Snowsight as your signup admin:

   ```sql
   ALTER USER HOL_USER ROTATE PROGRAMMATIC ACCESS TOKEN HOL_PAT;
   ```

   That returns a new `token_secret`. Copy *that* into `secret.pat`. Do not try to create the token by
   prompting Cortex Code; it is not connected yet.

4. **Get your account identifier.** Run this and copy the result:

   ```sql
   SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
   ```

   **Checkpoint:** `SHOW USERS LIKE 'HOL_USER'` returns one row, and you have the token text saved in
   `secret.pat` and the account identifier on your clipboard.

## C. Connect Cortex Code Desktop as HOL_USER

5. **Add the connection** using the `account_identifier` from step 4, user `HOL_USER`, authentication
   **Password**, and the token from `secret.pat` pasted into the password field. Role `ACCOUNTADMIN`.

6. **Confirm it.** This prompt also names a skill explicitly, which you can do by typing `/` and picking
   it from the list, or by naming it in the sentence (see [How the skills work](#how-the-skills-work)):

   **Prompt:**

   ```text
   Use the streaming-cdc-iceberg-lab skill: test my connection and confirm you loaded.
   ```

   **Checkpoint:** user comes back as `HOL_USER`, role `ACCOUNTADMIN`, region starts with `AWS_`, and
   Cortex Code names the `streaming-cdc-iceberg-lab` skill as active. If it does not name the skill,
   close the folder and reopen it, and choose **Trust**.

## D. Set up the local environment

7. **Build the virtual environment, install the dependencies, and write the producer profile.**

   **Prompt:**

   ```text
   Set up the local environment: the venv, dependencies, and producer/profile.json.
   ```

   Both requirement sets install: `producer/requirements.txt` and `external/requirements.txt` for
   Part 6, so nothing installs during the session. Three packages, about fifteen seconds. Your token
   is never printed to the chat.

   **Checkpoint:** `producer/profile.json` exists with five keys, and both of these succeed without
   touching Snowflake:

   ```bash
   .venv/bin/python producer/main.py --dry-run --cdc --seed 42 --duration 3
   .venv/bin/python -c "import pyiceberg, snowflake.connector; print('deps ok')"
   ```

---

# Run the lab

The six Parts map to three acts. Do the optional acts if you are ahead.

| Act | Parts | What you do in them |
|---|---|---|
| 1 — Real-time ingestion | 1, 2 | Create the Iceberg targets, then land both feeds in them |
| 2 — Continuous transformation | 3 | Layer four Dynamic Tables over both feeds |
| 3 — Serve it: to an agent, and to any engine | 4, 5, 6 | Ask it questions, revise the history behind the answers, then read the tables from outside Snowflake |

**Prompt** blocks are what you paste into Cortex Code, not SQL and not a shell command. **Fast path**
blocks are the finished SQL for the same step, and `solutions/` holds one file per Part. If you fall
behind, run the file and catch up; you will still hit every checkpoint. Each Part opens with how closely
to review what Cortex Code proposes.

## Part 1 — Land both feeds in Iceberg

**Approach: generate then confirm.** This Part creates every object the rest of the lab stands on, and
one of them has a failure mode that only surfaces four Parts later. Read the DDL before you approve
it.

Use **Plan Mode** (`Shift+Tab`) here. Cortex Code lays out the whole sequence before executing, so you
can see the `USE SCHEMA` statements below in context. Read the plan, then click **Build**
(`Cmd+Shift+B`) to run it — in Plan Mode nothing executes until you do.

**Prompt:**

```text
Create the lab environment, the telemetry landing table, and the simulator control table.
```

Two schemas, so provenance reads off any fully-qualified name: `RAW` arrived from outside, `ANALYTICS`
was computed for you.

| Schema | Holds | Why |
|---|---|---|
| `MFG.RAW` | The CDC destination table, its journal and stream, and the telemetry table | Both feeds land here. The journal sits beside its destination table because that is where the real Openflow connector puts it. |
| `MFG.ANALYTICS` | All four Dynamic Tables, the semantic view, the agent | Everything derived. Nothing writes here; Snowflake maintains all of it. |

**Two rules put every Iceberg object on v3, and both are in the DDL you are about to approve.**

1. Set all three defaults — `EXTERNAL_VOLUME`, `CATALOG` and `ICEBERG_VERSION_DEFAULT = 3` — on
   **`MFG.RAW` and `MFG.ANALYTICS` both**. The Dynamic Tables you create in Part 3 land in
   `MFG.ANALYTICS` and take their format version from that schema.
2. Issue a **`USE SCHEMA` before each `CREATE ICEBERG TABLE`**, matching the schema you are creating
   into. Watch for one above the telemetry table.

Confirm the version on the created table itself, with `iceberg_table_format_version`. The preflight
below does that for every object. If something lands on v2, recreate it.
[Troubleshooting](docs/troubleshooting.md) has the details.

The telemetry table's DDL contains no `CATALOG`, no `EXTERNAL_VOLUME` and no version. It inherits all
three from the schema.

**Checkpoint:** `SHOW ICEBERG TABLES IN DATABASE MFG` lists exactly one table, `STATION_TELEMETRY`, and
`SHOW TABLES IN SCHEMA MFG.RAW` lists `SIMULATOR_CONTROL` beside it. No `QUALITY_INSPECTIONS` yet: the
connector creates that table, the change journal and the journal's stream when you start it in Part 2.

Now verify everything, before building anything on top:

**Prompt:**

```text
Run the preflight checks.
```

**Checkpoint:** `aws_ok`, `cortex_ok`, `raw_iceberg_ok` and `analytics_iceberg_ok` all come back TRUE, and every
Iceberg object reports `is_v3 = TRUE`.
**Do not continue past a FALSE.** See [Troubleshooting](docs/troubleshooting.md). A wrong answer here gets
more expensive with every Part, so fix it now rather than after the next one.

## Part 2 — Watch the connector's change feed

**Approach: direct execution.** Everything here is read-only.

**Prompt:**

```text
Start the producer in the background with both sources, then verify rows are landing.
```

**Watch the first three lines it prints.** Before it streams anything, the connector creates its own
targets and says so:

```
10:02:14 [connector] destination table ready
10:02:14 [connector] journal ready
10:02:15 [connector] journal stream ready
```

Those three lines print whether the connector created the objects or found them already there. A managed
connector provisions its own destination tables; a streaming client does not, which is why you wrote
`STATION_TELEMETRY` yourself and not these three.

The destination table it just built is a **standard** table, not Iceberg, while its journal is Iceberg v3.
That is the connector's default — it writes standard destinations unless you opt into an Iceberg
destination format — and this lab keeps the default so you can watch a table being rewritten in place.
Everything downstream of it is Iceberg.

**Start it once and leave it running for the rest of the lab.** You never stop or restart it; Part 5
changes the data at the source instead.

**Keep its output visible.** It reports the plant floor every 15 seconds, and every merge as it fires:

```
10:03:30 [telem] rows=1860 booth_humidity~44.0
10:04:00 [merge] gate fired: 122 rows applied in 1.5s (merges=1 rows_total=122)
10:04:00 [cdc] inserts=62 updates=0 soft_deletes=1
```

In Part 5 this log shows the incident several seconds before any query does.

Only the *connector* is simulated. See [What is real, and what is simulated](#what-is-real-and-what-is-simulated).

**In steady state:** telemetry climbs fastest by far, roughly 900 rows per 15 seconds against a couple
of scans per second, so expect telemetry in the tens of thousands while the journal is in the low
thousands. That ratio is correct, not a fault.

**Checkpoint:** journal events, destination rows and telemetry rows all climb when you re-run the
query. Telemetry lag is **~30 seconds** on this account, measured. Expected, not a fault — and worth
knowing as a planning number, because it is the floor under the CDC path too: lowering the merge gate
below it buys you nothing.

### While that first 30 seconds passes

**Prompt:**

```text
Show me the pipes and channels for these tables.
```

**Checkpoint:** one pipe per target table, each with a name you did not choose, and no `CREATE PIPE`
anywhere in this lab or in `solutions/`. Snowpipe Streaming auto-created them.

**Prompt:**

```text
Show me the journal's change events, the event-type mix, and the destination's lag.
```

Three `EVENT_TYPE` values come back: inserts, updates and deletes. Updates are what Part 5's recovery
rides on. For the payload and key semantics of each, and why the MERGE needs an `IFF` on the delete case,
see [How the CDC connector works](docs/cdc-internals.md).

**In steady state:** the gap is roughly one minute of change events, around a hundred rows at the
default rate. It grows until the gate fires, drops, and grows again. A gap that never shrinks means
the merge is not running; a gap of zero means you are looking between a merge and its next batch.

**Checkpoint:** the journal count **exceeds** the destination count. That gap is the merge gate, not a
backlog. Each merge starts at second **:00** of a minute and finishes in a second or two — read that
straight off the producer's log, where every `[merge] gate fired` line is stamped `:00`. So the
latency here is a schedule you chose, not a throughput limit.

**Build on the destination table, `QUALITY_INSPECTIONS`, never on the journal.** The journal is
connector-internal: its name carries a registration timestamp and a generation counter that changes
when the source schema changes, and the connector never cleans it up — that part is yours. Part 3 reads
`QUALITY_INSPECTIONS`.

## Part 3 — Refine it with Dynamic Tables

**Approach: generate then confirm.** One predicate in here is the difference between a correct
pipeline and a plausible-looking wrong one. Read for it.

**Prompt:**

```text
Create two Dynamic Iceberg Tables in MFG.ANALYTICS, target lag 1 minute,
refresh mode INCREMENTAL.

INSPECTIONS_ACTIVE: the business columns from MFG.RAW.QUALITY_INSPECTIONS, not
the connector's bookkeeping columns, excluding rows it has soft-deleted, plus
IS_SCRAP as 1 or 0.

STATION_HEALTH: from MFG.RAW.STATION_TELEMETRY, one row per STATION_ID, LINE,
METRIC and 5-minute BUCKET, with READINGS, AVG_VALUE and MAX_VALUE.
```

`INSPECTIONS_ACTIVE` carries the predicate that matters: `WHERE NOT _SNOWFLAKE_DELETED`. A CDC
destination retains deleted rows and marks them, so anything built on one needs this predicate or it
counts rows the source has already retracted. Here: omit it and voided frames count against yield
forever. Part 5 depends on it.

**In steady state:** `INSPECTIONS_ACTIVE` tracks the destination table closely, a little smaller,
because soft-deleted rows are filtered out. `STATION_HEALTH` is tiny by comparison: it is one row per
station, metric and 5-minute bucket, so a few dozen rows is right even with tens of thousands of
readings underneath.

**Checkpoint:** `SHOW DYNAMIC TABLES` reports `refresh_mode = INCREMENTAL`, `is_iceberg = true`, and
an **empty** `refresh_mode_reason` for both. A populated `refresh_mode_reason` names its own cause.
Read it rather than guessing.

**Prompt:**

```text
Create two more Dynamic Iceberg Tables in MFG.ANALYTICS, same lag and
refresh mode.

YIELD_BY_LINE_5MIN: per LINE and 5-minute BUCKET from INSPECTIONS_ACTIVE, give
UNITS, SCRAP_UNITS and FIRST_PASS_YIELD_PCT rounded to 2 decimals; left join
STATION_HEALTH on the same line and bucket for AVG_BOOTH_HUMIDITY.

DEFECT_COUNTS_5MIN: per LINE, 5-minute BUCKET and DEFECT_CODE from
INSPECTIONS_ACTIVE, give N, counting no-defect rows as NONE.
```

Yield and booth humidity land in the same row for the same 5-minute interval. That join is what makes the
agent's causal answer possible in Part 5.

`AVG_BOOTH_HUMIDITY` is empty for WELD and ASSEMBLY. That is correct; booth humidity is a paint-booth
metric.

**In steady state:** each line sits around **95–99%** first-pass yield, with a handful of scrap units
per bucket. `AVG_BOOTH_HUMIDITY` reads about **44** for PAINT and is empty for WELD and ASSEMBLY.
Single-digit row counts are correct: three lines times the number of elapsed 5-minute buckets.

**Checkpoint:** all four Dynamic Tables now report `refresh_mode = INCREMENTAL`, and
`YIELD_BY_LINE_5MIN` holds three rows per elapsed 5-minute bucket, single digits early on. It is
much smaller than `QUALITY_INSPECTIONS`, as an aggregate should be.

Parts 4, 5 and 6 address these columns by name, so confirm them before moving on:

**Prompt:**

```text
Check the Dynamic Table column contract.
```

**Checkpoint:** every row reads `ok`. A `-- MISSING COLUMN --` or `-- WRONG TYPE --` names the table
and column to fix; re-run the prompt above for that table, naming that column. Fixing it here costs a
minute, and Part 4 would report the error against the semantic view instead of the table behind it.

**Prompt:**

```text
Show me the refresh history for these Dynamic Tables.
```

**Checkpoint:** the per-refresh row counts stay small even as the base table grows. Snowflake is
recomputing only the 5-minute groups that changed, while the source underneath is being UPDATEd and
DELETEd continuously by the connector's merges.

Snowflake ships its own Dynamic Tables skill. Ask it directly:

**Prompt:**

```text
Use the dynamic-tables skill: why is this refresh incremental, and what would break it?
```

**Checkpoint:** it names the operators that keep this query incremental, and the ones that would force a
full refresh.

It may suggest changing the refresh mode or the target lag — `ADAPTIVE`, `AUTO` or `DOWNSTREAM`. Do not:
this lab pins `INCREMENTAL` and a 1-minute lag on every layer, and the checkpoint above expects them.

### Where am I?

At any point, in any Part:

**Prompt:**

```text
Show me the lab progress query.
```

**Checkpoint:** it lists every object you should have built by now with its row count, and flags
anything missing.

## Part 4 — Ask it questions in English

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
succeeds and every question then fails with an opaque internal error naming neither the warehouse nor
the tool. Check the generated spec has it.

Now switch to **Snowsight → AI & ML → Agents → Cascade Plant Analyst** and use the chat panel on the
detail page. You do not need to Publish. Ask the first two questions from
[`docs/agent_questions.md`](docs/agent_questions.md):

1. *What is first-pass yield by line right now?*
2. *Which defect is driving scrap on PAINT?*

**Checkpoint:** the numbers match what the semantic view returned, and the agent names the 5-minute
interval it used. Keep this tab open; you need it in Part 5.

## Part 5 — The incident, and the recovery

**Approach: direct execution**, then read.

Any change feed has two hard properties: a **cause arrives before its effect**, and a **correction
arrives after the aggregate has already reported**. Here a paint booth drifts, then inspectors overturn
the frames it spoiled.

The producer is still running from Part 2, and it stays running. What changes is the **plant**, not the
pipeline. You write a row to a control table and the running simulator picks it up within about
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

**Checkpoint:** the producer's log shows `booth_humidity` climbing away from 44 within seconds
(`47.7`, `52.2`, `56.7`), and about 90 seconds later `[cdc] PAINT defect rate -> 26%`. Cause precedes
effect, visible before any query.

A real Openflow connector runs continuously: nobody restarts it because a booth misbehaved. The data
changes character at the source and the pipeline carries it through untouched.

Now watch the cascade arrive layer by layer, and time it:

| What | Where it shows up | When |
|---|---|---|
| Booth humidity climbs ~44 → ~70 | `STATION_HEALTH` | ~30–60 s |
| PAINT defects spike, `PAINT_RUN` dominates | `DEFECT_COUNTS_5MIN` | ~90 s later |
| PAINT yield falls into the **80s**, then the high 70s | `YIELD_BY_LINE_5MIN` | ~1–2 min after that |

WELD and ASSEMBLY stay around 96–97% throughout. They are your control.

In the Snowsight agent tab, ask question 3:

3. *Why did PAINT yield drop?*

**Checkpoint:** the agent connects the humidity rise to the `PAINT_RUN` defects and gets the **order**
right: humidity first, defects second.

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

Inspectors re-check failed frames and overturn them to PASS: an `UPDATE` arriving over CDC, through the
journal and the MERGE, that **rewrites history** — buckets that already reported now report better
numbers. It works because of two things you built earlier: the journal's `IncrementalUpdateRows` events
(Part 2) and `INSPECTIONS_ACTIVE`'s soft-delete predicate (Part 3).

**Checkpoint:** PAINT yield climbs back, including for earlier buckets, and the Dynamic Tables are still
`INCREMENTAL`. An append-only pipeline would have double-counted the frame or dropped the correction.

## Part 6 — Read it from your laptop

**Approach: run the script**, then read it.

PyIceberg reads the Gold Dynamic Table straight from object storage through the Horizon Catalog, using
vended credentials, and **no Snowflake warehouse computes the scan**. The dependencies are already in
your venv from Setup D.

```bash
.venv/bin/python external/read_iceberg.py
```

**Checkpoint:** it prints Iceberg format version `v3`, a storage path under Snowflake's managed bucket
(`s3://sfc-…-customer-interop-fs-…`), your rows, and a smaller row count after predicate pushdown on
`LINE == 'PAINT'`.

Horizon catalog access is billed as external-engine access, including when the reader is another
Snowflake account. Budget for it as you would any engine reading your lakehouse.

The auth path has two steps that are not in the PyIceberg docs, so read the script: it is short and
the comments explain both. To be walked through it instead:

**Prompt:**

```text
Use the iceberg-external-read skill: walk me through reading the Gold table.
```

---

# Optional acts

Both stand alone. Do either, both, or neither.

## Optional A — Look inside the connector

Both prompts show how you would audit a real Openflow deployment. The mechanics are in
[How the CDC connector works](docs/cdc-internals.md).

**Prompt:**

```text
Show me SF_METADATA, what type it really is, and pull the offset token out of it.
```

`SF_METADATA` is a `VARIANT` holding a JSON **string**. Parse it before you subscript it.

**Checkpoint:** `SF_METADATA:offset_token` returns `NULL` and `TYPEOF(SF_METADATA)` says `VARCHAR`,
while `PARSE_JSON(SF_METADATA::STRING):offset_token` returns an actual offset.

**Prompt:**

```text
Find the connector's merges in query history using its query tag.
```

Every merge carries a `QUERY_TAG` naming the application, operation and merge strategy. Filter
`QUERY_HISTORY` on it.

**Checkpoint:** roughly one MERGE per minute since you started the producer, each beginning at second
`:00` and finishing in a second or two. Many short merges on a schedule, not one long-running one.

## Optional B — Break it on purpose

**Prompt:**

```text
Add a top-defect column to DEFECT_COUNTS_5MIN using MODE(DEFECT_CODE).
```

**Checkpoint:** it fails at `CREATE` time, not at refresh time:
*"Change tracking is not supported on queries containing the function 'MODE'"*. That is why defects are
counted at their natural grain and ranked at read time instead.

## Optional C — Price the predicate

`INSPECTIONS_ACTIVE` excludes rows the connector soft-deleted. Read-only, thirty seconds, and it turns
that one-line predicate into a number.

**Prompt:**

```text
Show me what yield would report if INSPECTIONS_ACTIVE did not filter soft-deleted rows:
count QUALITY_INSPECTIONS with and without the predicate, and the yield each way.
```

**Checkpoint:** the two row counts differ, and so do the two yields. The gap is small and it never
closes — a voided scan stays in the table forever, so a pipeline built without that predicate is not
briefly wrong, it is permanently wrong by a margin that depends on how much your operators void. Nothing
in the data tells you it happened.

---


# How the skills work

`.snowflake/cortex/skills/` holds two Cortex Code **skills**. Both load **automatically** when you open
this folder. There is nothing to install, and nothing to type.

| Skill | What it carries |
|---|---|
| `streaming-cdc-iceberg-lab` | The object model, the measured Iceberg constraints, every checkpoint, and the rules for building each layer. It pins the conventions, so your prompts can state intent instead of restating them. |
| `iceberg-external-read` | Part 6 only: the Horizon catalog auth path and its two traps. Separate because it is a standalone activity that nothing else depends on. |

A prompt like *"Run the preflight checks"* contains no object names, no schema, no Iceberg settings. It
works because the skill already put all of that in context.

**Naming a skill explicitly.** Type `/` in the chat input and pick it from the list, or name it in the
sentence:

```text
Use the streaming-cdc-iceberg-lab skill: <your request>
```

You never have to: both auto-load. It is the lever for getting back on track — if a prompt produces the
wrong object name or ignores a constraint, name the skill in the next one. (In the Cortex Code CLI the
same thing is written `$streaming-cdc-iceberg-lab`.)

Read `SKILL.md` to see what a skill contains before you write one.

# Cleanup

**Stop the producer first** — `Ctrl-C` in the terminal it is running in, or ask Cortex Code to stop it if
it started it for you. That is the one that matters: while the producer runs, the warehouse never
suspends, a MERGE fires every minute and four Dynamic Tables keep refreshing. With it stopped there are
no upstream changes, so the Dynamic Tables stop waking the warehouse.

Then suspend the rest:

**Prompt:**

```text
Stop the spend: suspend the four Dynamic Tables and the warehouse, and keep my data.
```

**Checkpoint:** `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS` reports `scheduling_state = SUSPENDED` for all
four, or returns nothing at all if you removed them.

Or run [`solutions/09_cleanup.sql`](solutions/09_cleanup.sql) yourself: Block 1 stops the spend and
keeps your data, Block 2 removes everything. Then delete your local `secret.pat` and
`producer/profile.json`.

# License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

# Disclaimer

Sample code and content for educational purposes, provided as-is without warranty. Not intended for
production use. Cascade Cycleworks, the scenario, and all data in this lab are fictional.
