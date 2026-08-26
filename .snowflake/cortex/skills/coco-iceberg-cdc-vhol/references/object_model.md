# Object model — verbatim DDL

Emit these **exactly**. They are the same statements as the matching
`solutions/*.sql` files, which is the answer key the attendee can also read
directly. If a `CREATE` fails, re-emit from here rather than improvising syntax.

**Before every Iceberg `CREATE`, issue `USE SCHEMA`.** `ICEBERG_VERSION_DEFAULT`
resolves from the session's current schema, not from the schema holding the new
table, so omitting it silently produces an Iceberg **v2** table.

**Never create a task for the CDC merge.** The connector does not create one; the
producer issues the MERGE itself on a CRON gate. See the journal section below.

---

## 1. Environment and base tables

Source of truth: `solutions/01_environment.sql`

```sql
-- =====================================================================
-- 01_environment.sql   Answer key for Part 1
-- =====================================================================
-- You build this by PROMPTING Cortex Code. This file is what it should
-- produce. Use it to check your work, or to catch up if you fall behind.
--
-- The storage defaults below are the most load-bearing statements in the whole
-- lab. They make Iceberg tables resolve to Snowflake-managed storage at format
-- version 3 without any table-level clauses.
--
-- READ THIS, IT IS NOT WHAT YOU EXPECT (measured 26 Aug 2026):
--
--   EXTERNAL_VOLUME and CATALOG resolve from the schema that CONTAINS the new
--   table. ICEBERG_VERSION_DEFAULT resolves from the SESSION'S CURRENT SCHEMA.
--
-- So `CREATE ICEBERG TABLE MFG.RAW.T (...)` run without `USE SCHEMA MFG.RAW`
-- first gets the right volume and catalog but lands on **version 2**, even
-- though MFG.RAW has the version default set. SHOW PARAMETERS will cheerfully
-- report `value = 3, level = SCHEMA` the whole time. It is set, reported, and
-- ignored.
--
-- A v2 table is created successfully; the damage shows up later as
-- `Unsupported data type 'VARIANT'` or a rejected TIMESTAMP_NTZ(9) from
-- TIME_SLICE(), deep in the pipeline where the cause is invisible. And
-- CREATE DYNAMIC ICEBERG TABLE has no ICEBERG_VERSION clause at all, so for
-- the Dynamic Table layer there is no way to override it per statement.
--
-- Hence three layers of defence:
--   1. USE SCHEMA before every Iceberg create   <- the actual fix
--   2. the database-level default, so any schema inside MFG inherits v3
--   3. an explicit ICEBERG_VERSION = 3 wherever the syntax allows one
-- =====================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS MFG;
CREATE SCHEMA   IF NOT EXISTS MFG.RAW;        -- both landing zones: CDC destination, its journal, telemetry
CREATE SCHEMA   IF NOT EXISTS MFG.ANALYTICS;  -- everything derived: the Dynamic Tables, semantic view, agent

-- Layer 2: database level. Any session whose current schema is anywhere inside
-- MFG now inherits v3, which covers the case where you are in MFG.ANALYTICS and
-- create something in MFG.RAW.
ALTER DATABASE MFG SET ICEBERG_VERSION_DEFAULT = 3;

-- Storage defaults. Do this BEFORE creating any table.
ALTER SCHEMA MFG.RAW SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.RAW SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.RAW SET ICEBERG_VERSION_DEFAULT = 3;

-- ANALYTICS needs these too, and it is easy to think it does not: every object
-- in it is a Dynamic ICEBERG Table, and CREATE DYNAMIC ICEBERG TABLE has no
-- ICEBERG_VERSION clause at all. It inherits or it is wrong.
ALTER SCHEMA MFG.ANALYTICS SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.ANALYTICS SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.ANALYTICS SET ICEBERG_VERSION_DEFAULT = 3;

CREATE WAREHOUSE IF NOT EXISTS HOL_WH
  WAREHOUSE_SIZE = 'XSMALL'
  GENERATION = '2'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.RAW;

-- ---------------------------------------------------------------------
-- The CDC destination table is NOT created here.
--
-- MFG.RAW.QUALITY_INSPECTIONS, the change journal and the journal stream all
-- belong to the connector, and it creates them itself on first run -- see
-- producer/cdc_simulator.py, ensure_objects(). That is what a real Openflow
-- connector does: you point it at a source and the objects appear.
--
-- What stays here is what the connector does NOT own: the database, the schemas
-- and their Iceberg defaults, the warehouse, the telemetry table the attendee
-- streams into directly, and the simulator's control plane.
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
-- The simulator's control plane.
--
-- This is how you change the WORLD in Part 5 without touching the pipeline.
-- You write a row here; the running producer notices within ~10 seconds and
-- the plant floor starts behaving differently. Streaming never stops.
--
-- That is not a lab shortcut, it is what the real thing looks like. An
-- Openflow connector runs continuously. When a paint booth starts misbehaving
-- nobody restarts the connector -- the data changes character at the source and
-- the pipeline carries it through unchanged.
--
-- Deliberately a STANDARD table: it is operational metadata, not a feed and not
-- derived from one. Making it Iceberg would buy nothing and would add one more
-- schema where the ICEBERG_VERSION_DEFAULT session trap could bite.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MFG.RAW.SIMULATOR_CONTROL (
  MODE        STRING,          -- STEADY | INCIDENT | REINSPECT
  UPDATED_AT  TIMESTAMP_NTZ    -- newest row wins
);

-- Start the plant in a good mood.
INSERT INTO MFG.RAW.SIMULATOR_CONTROL (MODE, UPDATED_AT)
  SELECT 'STEADY', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  WHERE NOT EXISTS (SELECT 1 FROM MFG.RAW.SIMULATOR_CONTROL);

-- ---------------------------------------------------------------------
-- The streaming telemetry table: Iceberg, append-only.
--
-- Note what is NOT here: no CATALOG, no EXTERNAL_VOLUME, no ICEBERG_VERSION.
-- All three come from the defaults set above -- and the `USE SCHEMA MFG.RAW`
-- on the line before is what makes the VERSION one work. Without it this
-- table lands on v2 while still reporting the correct volume and catalog.
-- 02_preflight.sql verifies the actual result rather than trusting it.
--
-- Snowpipe Streaming will auto-create a default pipe named
-- STATION_TELEMETRY-STREAMING for this table. You never write CREATE PIPE.
-- ---------------------------------------------------------------------
USE SCHEMA MFG.RAW;

CREATE OR REPLACE ICEBERG TABLE MFG.RAW.STATION_TELEMETRY (
  STATION_ID  STRING,
  LINE        STRING,
  METRIC      STRING,          -- weld_current | booth_humidity | booth_temp | torque_nm
  VALUE       DOUBLE,
  EVENT_TS    TIMESTAMP_NTZ
);

USE SCHEMA MFG.RAW;
```

---

## 2. CDC journal and append-only stream

> **Created by the connector, not by the attendee.** `producer/cdc_simulator.py`
> (`ensure_objects()`) creates the destination table, the journal and the stream on first
> run, as a real Openflow connector does. Everything below is reference: emit it only if
> someone explicitly asks to see or hand-build the DDL.


Source of truth: `solutions/03_journal_inspection.sql`

Two objects only — no task. The MERGE the producer issues is included as a
commented reference so it can be read and run by hand.

```sql
-- =====================================================================
-- 03_journal_inspection.sql   Answer key for Part 2 (inspect only -- the connector builds)
-- =====================================================================
-- This is the part of the lab that is actually about change data capture.
--
-- The Openflow PostgreSQL CDC connector does NOT write your destination table
-- directly. It writes a JOURNAL -- an append-only log of change events -- and a
-- merge processor applies that journal to the destination.
--
-- IMPORTANT, because it is easy to assume otherwise: the connector does **not**
-- create a Snowflake TASK. Its merge processor runs inside the connector runtime
-- and issues the MERGE itself, over its own Snowflake connection. A CRON
-- expression acts as an internal *eligibility gate* deciding when queued changes
-- become mergeable -- the flow default is `0 * * * * ?`, second :00 of every
-- minute. So in this lab the producer issues the MERGE too, on the same gate.
-- There is no task to create here, and you should not add one.
--
-- Three observable behaviours come out of that design, and all three are worth
-- seeing:
--
--   1. Soft deletes. A voided row is flagged, never removed.
--   2. A merge gate. The destination lags the journal by up to a minute, and
--      that lag is a *scheduling* decision, not a throughput limit.
--   3. Two paths. The initial snapshot loads the destination directly; ongoing
--      changes go through the journal. Same table, two very different writers.
--
-- You INSPECT the journal in this lab. You never build Dynamic Tables on it --
-- it is connector-internal, its schema shifts with the generation counter, and
-- the connector prunes it. Build on the destination table.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.RAW;          -- required before any Iceberg CREATE; see 01_environment.sql

-- ---------------------------------------------------------------------
-- The journal table.
--
-- Naming is the connector's: "<TABLE>_JOURNAL_<series>_<generation>", where
-- series is epoch seconds at table registration and generation starts at 1 and
-- increments on every schema-changing DDL. Only the highest generation is
-- active. We PIN the series here so the lab has stable object names; in
-- production you cannot predict it, which is part of why you never build
-- anything durable on the journal.
--
-- Column order is the connector's and matters:
--   PRIMARY_KEY__<col>  one per replication key column, NOT NULL
--   PAYLOAD__<col>      one per EVERY source column, including the key
--   LEAST_/MOST_SIGNIFICANT_POSITION   the WAL position, used for ordering
--   EVENT_TYPE, SEEN_AT, SF_METADATA
--
-- The key appears TWICE, as PRIMARY_KEY__INSPECTION_ID and PAYLOAD__INSPECTION_ID. That is
-- deliberate: it is what makes a primary-key change detectable
-- (PRIMARY_KEY__k <> PAYLOAD__k). This lab's key never changes, so the MERGE
-- below omits the key-change machinery -- see the note at the end.
--
-- SF_METADATA is VARIANT, which is exactly why this table needs Iceberg v3.
-- ICEBERG_VERSION = 3 is stated explicitly rather than inherited, because a
-- silent fall back to v2 here fails with "Unsupported data type 'VARIANT'".
-- ---------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1 (
  PRIMARY_KEY__INSPECTION_ID        STRING        NOT NULL,
  PAYLOAD__INSPECTION_ID            STRING,
  PAYLOAD__UNIT_ID           STRING,
  PAYLOAD__LINE               STRING,
  PAYLOAD__SKU                STRING,
  PAYLOAD__STATUS             STRING,
  PAYLOAD__DEFECT_CODE        STRING,
  PAYLOAD__STATION_ID         STRING,
  PAYLOAD__OPERATOR_ID        STRING,
  PAYLOAD__EVENT_TS           TIMESTAMP_NTZ,
  PAYLOAD__UPDATED_TS         TIMESTAMP_NTZ,
  LEAST_SIGNIFICANT_POSITION  NUMBER(38,0),   -- bare NUMBER is rejected by Iceberg
  MOST_SIGNIFICANT_POSITION   NUMBER(38,0),
  EVENT_TYPE                  STRING        NOT NULL,
  SEEN_AT                     TIMESTAMP_NTZ,
  SF_METADATA                 VARIANT
)
  ICEBERG_VERSION = 3
  ERROR_LOGGING = TRUE;      -- the connector sets this; bad rows land in an error table

-- ---------------------------------------------------------------------
-- The stream. APPEND_ONLY, because a journal only ever gets appends.
--
-- Reading the STREAM rather than the table is what gives exactly-once: the
-- offset advances only when the consuming DML commits. Note that append-only
-- streams on *externally managed* Iceberg tables are unsupported -- on
-- Snowflake-managed v3, as here, they work.
-- ---------------------------------------------------------------------
CREATE OR REPLACE STREAM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM
  ON TABLE MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
  APPEND_ONLY = TRUE;

-- That is all the DDL. Two objects. No task, no pipe.


-- =====================================================================
-- THE MERGE -- for reference, and to run by hand
-- =====================================================================
-- The producer issues this statement on its CRON gate, exactly as the connector
-- does, with the connector's QUERY_TAG set so you can find it in QUERY_HISTORY.
-- You do not need to run it yourself; it is here so you can read it, and so you
-- can apply a batch on demand if you want to see the effect immediately.
--
-- Invariants it preserves, all of which matter:
--   1. Read the STREAM, not the journal table. Consuming the stream inside a
--      committed statement is what advances the offset.
--   2. Dedup to ONE row per replication key, ordered by the LSN tuple DESC.
--      Without this, an insert-then-update in the same batch applies in
--      arbitrary order.
--   3. Soft delete only -- never DELETE FROM the destination.
--   4. The ON clause joins SOURCE.PRIMARY_KEY__<k> to TARGET.<k>, prefix stripped.
--   5. The INSERT branch falls back to PRIMARY_KEY__ for delete tombstones,
--      because a DELETE event carries no payload at all.
--
-- ALTER SESSION SET QUERY_TAG = '{"application":"SNOWFLAKE_OPENFLOW","operation":"cdc.merge.full_values","strategy":"full_values_snowflake_managed"}';
--
-- MERGE INTO MFG.RAW.QUALITY_INSPECTIONS AS TARGET
-- USING (
--     SELECT * FROM (
--         SELECT PRIMARY_KEY__INSPECTION_ID,
--                PAYLOAD__INSPECTION_ID, PAYLOAD__UNIT_ID, PAYLOAD__LINE, PAYLOAD__SKU,
--                PAYLOAD__STATUS, PAYLOAD__DEFECT_CODE, PAYLOAD__STATION_ID,
--                PAYLOAD__OPERATOR_ID, PAYLOAD__EVENT_TS, PAYLOAD__UPDATED_TS,
--                EVENT_TYPE,
--                ROW_NUMBER() OVER (
--                    PARTITION BY PRIMARY_KEY__INSPECTION_ID
--                    ORDER BY MOST_SIGNIFICANT_POSITION DESC,
--                             LEAST_SIGNIFICANT_POSITION DESC
--                ) AS ROW_NUM
--         FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM
--         WHERE EVENT_TYPE IN ('IncrementalInsertRows',
--                              'IncrementalUpdateRows',
--                              'IncrementalDeleteRows')
--     ) WHERE ROW_NUM = 1
-- ) AS SOURCE
-- ON SOURCE.PRIMARY_KEY__INSPECTION_ID = TARGET.INSPECTION_ID
-- WHEN MATCHED AND SOURCE.EVENT_TYPE IN ('IncrementalInsertRows','IncrementalUpdateRows') THEN
--     UPDATE SET TARGET.INSPECTION_ID               = SOURCE.PAYLOAD__INSPECTION_ID,
--                TARGET.UNIT_ID              = SOURCE.PAYLOAD__UNIT_ID,
--                TARGET.LINE                  = SOURCE.PAYLOAD__LINE,
--                TARGET.SKU                   = SOURCE.PAYLOAD__SKU,
--                TARGET.STATUS                = SOURCE.PAYLOAD__STATUS,
--                TARGET.DEFECT_CODE           = SOURCE.PAYLOAD__DEFECT_CODE,
--                TARGET.STATION_ID            = SOURCE.PAYLOAD__STATION_ID,
--                TARGET.OPERATOR_ID           = SOURCE.PAYLOAD__OPERATOR_ID,
--                TARGET.EVENT_TS              = SOURCE.PAYLOAD__EVENT_TS,
--                TARGET.UPDATED_TS            = SOURCE.PAYLOAD__UPDATED_TS,
--                TARGET._SNOWFLAKE_DELETED    = FALSE,
--                TARGET._SNOWFLAKE_UPDATED_AT = SYSDATE()
-- WHEN MATCHED AND SOURCE.EVENT_TYPE = 'IncrementalDeleteRows' THEN
--     UPDATE SET TARGET._SNOWFLAKE_DELETED    = TRUE,
--                TARGET._SNOWFLAKE_UPDATED_AT = SYSDATE()
-- WHEN NOT MATCHED THEN
--     INSERT (INSPECTION_ID, UNIT_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID,
--             OPERATOR_ID, EVENT_TS, UPDATED_TS,
--             _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
--     VALUES (IFF(SOURCE.EVENT_TYPE = 'IncrementalDeleteRows',
--                 SOURCE.PRIMARY_KEY__INSPECTION_ID, SOURCE.PAYLOAD__INSPECTION_ID),
--             SOURCE.PAYLOAD__UNIT_ID, SOURCE.PAYLOAD__LINE, SOURCE.PAYLOAD__SKU,
--             SOURCE.PAYLOAD__STATUS, SOURCE.PAYLOAD__DEFECT_CODE,
--             SOURCE.PAYLOAD__STATION_ID, SOURCE.PAYLOAD__OPERATOR_ID,
--             SOURCE.PAYLOAD__EVENT_TS, SOURCE.PAYLOAD__UPDATED_TS,
--             SYSDATE(), SYSDATE(),
--             IFF(SOURCE.EVENT_TYPE = 'IncrementalDeleteRows', TRUE, FALSE));


-- =====================================================================
-- CHECKPOINTS -- run these while the producer is going
-- =====================================================================

-- 1. Raw change events, newest first. This is the CDC wire format.
--    Look at what each EVENT_TYPE carries:
--      IncrementalInsertRows  every PAYLOAD__* populated
--      IncrementalUpdateRows  PAYLOAD__* holds the NEW values
--      IncrementalDeleteRows  every PAYLOAD__* is NULL -- key only
SELECT EVENT_TYPE, PRIMARY_KEY__INSPECTION_ID, PAYLOAD__STATUS, PAYLOAD__DEFECT_CODE,
       MOST_SIGNIFICANT_POSITION AS txn_lsn, LEAST_SIGNIFICANT_POSITION AS msg_lsn, SEEN_AT
FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
ORDER BY MOST_SIGNIFICANT_POSITION DESC, LEAST_SIGNIFICANT_POSITION DESC
LIMIT 20;

-- 2. Event mix.
SELECT EVENT_TYPE, COUNT(*) AS n
FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
GROUP BY 1 ORDER BY 2 DESC;

-- 3. SF_METADATA is a VARIANT that holds a JSON *string*, not a parsed object --
--    the connector writes it that way. So this returns NULL:
--        SELECT SF_METADATA:offset_token FROM ...
--    and this works:
SELECT SF_METADATA                                          AS raw_variant,
       TYPEOF(SF_METADATA)                                  AS what_it_really_is,
       PARSE_JSON(SF_METADATA::STRING):offset_token::STRING  AS offset_token
FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
LIMIT 3;

-- 4. THE MERGE GATE, which is the point of this section.
--    The journal always leads the destination. The gap is the scheduling gate,
--    not a throughput limit -- the merge itself takes a second or two.
SELECT (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
         WHERE EVENT_TYPE = 'IncrementalInsertRows')          AS journal_inserts,
       (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS)        AS destination_rows,
       (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
         WHERE EVENT_TYPE = 'IncrementalInsertRows')
         - (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS)    AS awaiting_merge,
       SYSTEM$STREAM_HAS_DATA(
         'MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM')
                                                              AS stream_has_pending;

-- 5. The merges themselves, found the way you would find them in production:
--    by the connector's QUERY_TAG. This is the payoff of the tag, and it is
--    exactly how you would audit a real Openflow deployment.
--
--    Note the START_TIME of each one: second :00, every minute. That is the CRON
--    eligibility gate. Note also TOTAL_ELAPSED_TIME -- a second or two. The
--    latency is the gate, not the merge.
SELECT TO_VARCHAR(START_TIME, 'HH24:MI:SS')        AS fired_at,
       QUERY_TYPE,
       ROWS_PRODUCED                              AS rows_affected,
       ROUND(TOTAL_ELAPSED_TIME / 1000, 1)        AS merge_seconds,
       WAREHOUSE_NAME,
       PARSE_JSON(QUERY_TAG):operation::STRING    AS connector_operation,
       PARSE_JSON(QUERY_TAG):strategy::STRING     AS merge_strategy
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 500))
WHERE QUERY_TAG LIKE '%SNOWFLAKE_OPENFLOW%'
ORDER BY START_TIME DESC
LIMIT 10;

-- 6. Soft delete, end to end. A voided scan is still physically in the
--    destination, flagged. Everything downstream must filter it -- which is
--    exactly what INSPECTIONS_ACTIVE does in the next file.
SELECT INSPECTION_ID, LINE, STATUS, DEFECT_CODE, _SNOWFLAKE_DELETED,
       _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT
FROM MFG.RAW.QUALITY_INSPECTIONS
WHERE _SNOWFLAKE_DELETED
LIMIT 5;

-- 7. An overturned frame: _SNOWFLAKE_UPDATED_AT has moved past
--    _SNOWFLAKE_INSERTED_AT, and STATUS is now PASS with no defect code. The
--    MERGE rewrote a row that had already been reported on.
SELECT INSPECTION_ID, LINE, STATUS, DEFECT_CODE,
       _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT
FROM MFG.RAW.QUALITY_INSPECTIONS
WHERE _SNOWFLAKE_UPDATED_AT > _SNOWFLAKE_INSERTED_AT AND NOT _SNOWFLAKE_DELETED
LIMIT 5;

-- 8. Tighten the gate and watch the lag fall. This is the honest lesson about
--    where the latency actually lives: it is a schedule, and you choose it.
--    Restart the producer with a shorter gate and re-run checkpoint 4:
--
--      producer.py --profile producer/profile.json --cdc --telemetry \
--                  --merge-gate-seconds 10
--
--    In a real deployment this is the connector's `Merge Task Schedule CRON`
--    parameter. The trade is warehouse time: more merges, smaller batches.


-- =====================================================================
-- WHAT THIS SIMULATION LEAVES OUT -- stated, not hidden
-- =====================================================================
-- 1. PRIMARY KEY CHANGES. A real connector handles an UPDATE that changes the
--    replication key by turning it into a soft-delete of the old key plus an
--    insert of the new one, detected with PRIMARY_KEY__k <> PAYLOAD__k and
--    materialised with a CROSS JOIN that splits the row in two. INSPECTION_ID never
--    changes in this lab, so that machinery is omitted for readability.
-- 2. SCHEMA EVOLUTION. A schema-changing DDL writes a SchemaChangedRows marker,
--    bumps the generation, and creates a NEW journal table and stream. We pin
--    generation 1.
-- 3. THE SNAPSHOT PATH. A real connector first bulk-loads the destination table,
--    then switches to the journal for ongoing changes. Here the producer starts
--    from an empty table, so every row arrives as CDC.
-- 4. CONCURRENCY. The real merge processor runs up to 4 concurrent tasks with
--    per-table mutual exclusion and async query submission with retries. The
--    producer runs one merge at a time, serially.
-- 5. JOURNAL PRUNING, re-snapshot archive tables, and the TOAST / unchanged-value
--    placeholder handling for wide source rows.
-- 6. Real WAL LSNs. The producer uses a monotonic logical clock instead, which is
--    all the ordering guarantee the MERGE actually requires.
```

---

## 3. Dynamic Iceberg Table DAG

Source of truth: `solutions/04_dynamic_tables.sql`

```sql
-- =====================================================================
-- 04_dynamic_tables.sql   Answer key for Part 3
-- =====================================================================
-- The pipeline. Four Dynamic ICEBERG Tables, every one of them refreshing
-- INCREMENTALLY -- Snowflake recomputes only the groups that changed, not
-- the whole table, even though the CDC source underneath is being UPDATEd
-- and DELETEd continuously.
--
--   QUALITY_INSPECTIONS (standard, mutating)   STATION_TELEMETRY (Iceberg, append)
--             |                                          |
--             v                                          v
--     INSPECTIONS_ACTIVE                          STATION_HEALTH
--     (soft deletes filtered)                  (5-min metric rollup)
--             |                                          |
--             +---------------------+--------------------+
--                                   v
--                      YIELD_BY_LINE_5MIN     <- the two-source join
--                                   |
--                                   v
--                      DEFECT_COUNTS_5MIN
--
-- THINGS THAT WILL COST YOU TIME, all of them measured on a real account:
--
--  * TIME_SLICE() returns TIMESTAMP_NTZ(9). Iceberg v2 rejects scale 9. On an
--    all-v3 chain a bare TIME_SLICE() is accepted, but the ::TIMESTAMP_NTZ(6)
--    cast below is kept deliberately: it costs nothing and it keeps working if
--    a schema default is ever missed. CREATE DYNAMIC ICEBERG TABLE has no
--    version clause, so it can only inherit ICEBERG_VERSION_DEFAULT.
--
--  * MODE() is a hard CREATE error under change tracking, not a slow path:
--    "Change tracking is not supported on queries containing the function 'MODE'".
--    That is why "top defect" is a grain here and derived at read time.
--
--  * OBJECT / OBJECT_AGG output cannot land in an Iceberg table on v2 OR v3.
--
--  * Pin TARGET_LAG on every layer. TARGET_LAG = DOWNSTREAM inherits from the
--    consumer, so a "1 minute" pipeline can quietly run at the consumer's lag.
--
--  * APPROX_PERCENTILE forces a FULL refresh. Avoid it here.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.RAW;

-- ---------------------------------------------------------------------
-- Layer 1a: the soft-delete filter.
--
-- The connector never hard-deletes. A voided scan is still physically present
-- with _SNOWFLAKE_DELETED = TRUE. If you skip this WHERE clause, voided frames
-- keep counting against yield forever. This one predicate is the difference
-- between a correct CDC pipeline and a plausible-looking wrong one.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.ANALYTICS.INSPECTIONS_ACTIVE
  TARGET_LAG = '1 minute'
  WAREHOUSE = HOL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT INSPECTION_ID, UNIT_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID, OPERATOR_ID,
       EVENT_TS, UPDATED_TS,
       IFF(STATUS = 'FAIL', 1, 0) AS IS_SCRAP
FROM MFG.RAW.QUALITY_INSPECTIONS
WHERE NOT _SNOWFLAKE_DELETED;

-- ---------------------------------------------------------------------
-- Layer 1b: telemetry rolled up to the SAME 5-minute grain as yield, which
-- is what makes the join in the next layer possible.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.ANALYTICS.STATION_HEALTH
  TARGET_LAG = '1 minute'
  WAREHOUSE = HOL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT STATION_ID, LINE, METRIC,
       TIME_SLICE(EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6) AS BUCKET,
       COUNT(*)   AS READINGS,
       AVG(VALUE) AS AVG_VALUE,
       MAX(VALUE) AS MAX_VALUE
FROM MFG.RAW.STATION_TELEMETRY
GROUP BY 1, 2, 3, 4;

-- ---------------------------------------------------------------------
-- Gold 1: yield per line per 5 minutes, WITH the booth metric alongside it.
--
-- This is the join that earns the second data source. Yield alone tells you
-- PAINT is scrapping frames; yield next to humidity tells you WHY, and that
-- is the difference between an agent that reports and an agent that explains.
--
-- AVG_BOOTH_HUMIDITY is NULL for WELD and ASSEMBLY, which is correct --
-- booth humidity is a paint-booth metric. The LEFT JOIN keeps those lines
-- in the result instead of dropping them.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.ANALYTICS.YIELD_BY_LINE_5MIN
  TARGET_LAG = '1 minute'
  WAREHOUSE = HOL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT s.LINE,
       TIME_SLICE(s.EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6)   AS BUCKET,
       COUNT(*)                                                AS UNITS,
       SUM(s.IS_SCRAP)                                         AS SCRAP_UNITS,
       ROUND(100 * (COUNT(*) - SUM(s.IS_SCRAP)) / COUNT(*), 2) AS FIRST_PASS_YIELD_PCT,
       AVG(h.AVG_VALUE)                                        AS AVG_BOOTH_HUMIDITY
FROM MFG.ANALYTICS.INSPECTIONS_ACTIVE s
LEFT JOIN MFG.ANALYTICS.STATION_HEALTH h
       ON h.LINE   = s.LINE
      AND h.METRIC = 'booth_humidity'
      AND h.BUCKET = TIME_SLICE(s.EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6)
GROUP BY 1, 2;

-- ---------------------------------------------------------------------
-- Gold 2: defect counts at their natural grain.
--
-- The obvious way to write "what is the top defect" is MODE(DEFECT_CODE).
-- Try it -- it fails at CREATE time, not at refresh time. Counting at
-- (line, bucket, defect_code) and ranking at read time is both legal and
-- more useful, because it keeps the full distribution.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.ANALYTICS.DEFECT_COUNTS_5MIN
  TARGET_LAG = '1 minute'
  WAREHOUSE = HOL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT LINE,
       TIME_SLICE(EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6) AS BUCKET,
       COALESCE(DEFECT_CODE, 'NONE')                       AS DEFECT_CODE,
       COUNT(*)                                            AS N
FROM MFG.ANALYTICS.INSPECTIONS_ACTIVE
GROUP BY 1, 2, 3;


-- =====================================================================
-- CHECKPOINTS
-- =====================================================================

-- Every row must read INCREMENTAL / true, and DOWNGRADE_REASON must be empty.
-- If refresh_mode came back FULL, something in the query blocked incremental
-- refresh and refresh_mode_reason will say what.
SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS
  ->> SELECT "name", "refresh_mode", "is_iceberg", "target_lag", "scheduling_state",
             NULLIF("refresh_mode_reason", '') AS downgrade_reason
      FROM $1 ORDER BY "name";

-- Yield by line, most recent buckets. During the incident PAINT falls well
-- below the other two lines and AVG_BOOTH_HUMIDITY climbs in the same bucket.
SELECT LINE, BUCKET, UNITS, SCRAP_UNITS, FIRST_PASS_YIELD_PCT,
       ROUND(AVG_BOOTH_HUMIDITY, 1) AS HUMIDITY
FROM MFG.ANALYTICS.YIELD_BY_LINE_5MIN
ORDER BY BUCKET DESC, LINE
LIMIT 12;

-- Top defect, derived at read time (the MODE() replacement).
SELECT LINE, DEFECT_CODE, SUM(N) AS N
FROM MFG.ANALYTICS.DEFECT_COUNTS_5MIN
WHERE DEFECT_CODE <> 'NONE'
  AND BUCKET >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY N DESC
LIMIT 5;

-- Proof that incremental refresh is doing real delta work rather than
-- recomputing. Look at the inserted/deleted row counts per refresh: on an
-- aggregate over a mutating CDC source they stay small even as the base
-- table grows, because only the changed 5-minute groups are recomputed.
SELECT NAME, REFRESH_START_TIME, REFRESH_ACTION, STATE, STATISTICS
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
        NAME_PREFIX => 'MFG.ANALYTICS.'))
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;


-- =====================================================================
-- THE NEGATIVE EXAMPLE (Optional B) -- run it and read the error
-- =====================================================================
-- This is a real, instructive failure, not a contrived one. It is the first
-- thing most people reach for.
--
-- CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.ANALYTICS.TOP_DEFECT_BROKEN
--   TARGET_LAG = '1 minute' WAREHOUSE = HOL_WH REFRESH_MODE = INCREMENTAL
-- AS
-- SELECT LINE,
--        TIME_SLICE(EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6) AS BUCKET,
--        MODE(DEFECT_CODE) AS TOP_DEFECT
-- FROM MFG.ANALYTICS.INSPECTIONS_ACTIVE
-- GROUP BY 1, 2;
--
-- Expected:
--   Change tracking is not supported on queries containing the function 'MODE'
```

---

## 4. Semantic view

Source of truth: `solutions/05_semantic_view.sql`

**Syntax rules — all four have been generated wrong before:**
1. Clause order is fixed: `TABLES` → `RELATIONSHIPS` → `FACTS` → `DIMENSIONS` → `METRICS`
2. Tables use `AS`, never `=`
3. Synonyms use `WITH SYNONYMS = (...)`, never a bare `SYNONYMS = (...)`
4. Metrics are alias-qualified and defined with `AS`

```sql
-- =====================================================================
-- 05_semantic_view.sql   Answer key for Part 4
-- =====================================================================
-- The semantic view is what turns the Gold tables into something an agent can
-- reason about: business names, synonyms, and the metric definitions, so the
-- agent is not guessing at what a column means.
--
-- SYNTAX RULES. Cortex Code has historically generated all four of these wrong.
-- If a CREATE fails, re-emit this DDL verbatim rather than improvising:
--   1. Clause order is fixed: TABLES -> RELATIONSHIPS -> FACTS -> DIMENSIONS -> METRICS
--   2. Tables use AS, never '=':          yield AS MFG.ANALYTICS....
--   3. Synonyms use WITH SYNONYMS = (...), never a bare SYNONYMS = (...)
--   4. Metrics are alias-qualified and defined with AS:
--        yield.total_units AS SUM(yield.units)      -- correct
--        total_units = SUM(units)                   -- WRONG, will not compile
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.RAW;

-- THREE tables, not two. The agent needs stations even though
-- YIELD_BY_LINE_5MIN already carries booth humidity, because "is anything
-- wrong on WELD?"
-- is a question about a metric that never reaches the yield table.
CREATE OR REPLACE SEMANTIC VIEW MFG.ANALYTICS.PLANT_FLOOR_SV
  TABLES (
    yield AS MFG.ANALYTICS.YIELD_BY_LINE_5MIN
      PRIMARY KEY (LINE, BUCKET)
      WITH SYNONYMS = ('yield', 'first pass yield', 'production yield', 'line output')
      COMMENT = 'Units, scrap and first-pass yield per production line per 5 minutes, with the paint booth humidity for the same interval.',
    defects AS MFG.ANALYTICS.DEFECT_COUNTS_5MIN
      PRIMARY KEY (LINE, BUCKET, DEFECT_CODE)
      WITH SYNONYMS = ('defects', 'defect counts', 'scrap reasons', 'failure codes')
      COMMENT = 'Count of scans per defect code per line per 5 minutes. DEFECT_CODE = NONE means the scan passed.',
    stations AS MFG.ANALYTICS.STATION_HEALTH
      PRIMARY KEY (STATION_ID, METRIC, BUCKET)
      WITH SYNONYMS = ('stations', 'station health', 'sensors', 'telemetry', 'machine metrics')
      COMMENT = 'Sensor telemetry averaged per station per metric per 5 minutes.'
  )
  -- Both relationships point AT yield, making it the hub. That is deliberate:
  -- every question in this lab is ultimately "how is the line doing", so the
  -- agent should reach defects and telemetry by way of yield rather than
  -- joining them to each other. A star beats a chain for text-to-SQL, because
  -- there is only ever one join path to get wrong.
  RELATIONSHIPS (
    defects_to_yield AS defects (LINE, BUCKET) REFERENCES yield (LINE, BUCKET),
    stations_to_yield AS stations (LINE, BUCKET) REFERENCES yield (LINE, BUCKET)
  )
  -- FACTS are raw columns; METRICS are aggregations over them. The split
  -- matters: an agent handed only raw columns invents its own aggregations and
  -- picks a different one each time you ask. Naming the metric once here is
  -- what makes two runs of the same question return the same number.
  FACTS (
    yield.units AS UNITS,
    yield.scrap_units AS SCRAP_UNITS,
    yield.yield_pct AS FIRST_PASS_YIELD_PCT,
    yield.booth_humidity AS AVG_BOOTH_HUMIDITY,
    defects.defect_n AS N,
    stations.reading_avg AS AVG_VALUE,
    stations.reading_max AS MAX_VALUE
  )
  -- SYNONYMS are the highest-leverage thing in this file. A plant manager says
  -- "work centre" and "stage"; the column is called LINE. Every synonym you
  -- add is a question that now resolves without clarification. COMMENTs do
  -- the same job for values -- note the two places that spell out what NONE
  -- means, because "the top defect is NONE" is the classic wrong answer.
  DIMENSIONS (
    yield.line AS LINE
      WITH SYNONYMS = ('line', 'production line', 'work centre', 'stage')
      COMMENT = 'Production line: WELD, PAINT or ASSEMBLY.',
    yield.bucket AS BUCKET
      WITH SYNONYMS = ('time', 'interval', 'five minute bucket', 'when')
      COMMENT = 'Start of the 5-minute interval, UTC.',
    defects.defect_code AS DEFECT_CODE
      WITH SYNONYMS = ('defect', 'defect code', 'failure reason', 'scrap reason')
      COMMENT = 'Defect code, or NONE for a passing scan.',
    stations.station_id AS STATION_ID
      WITH SYNONYMS = ('station', 'machine', 'cell'),
    stations.metric AS METRIC
      WITH SYNONYMS = ('metric', 'sensor', 'measurement')
      COMMENT = 'One of weld_current, booth_humidity, booth_temp, torque_nm.'
  )
  METRICS (
    yield.total_units AS SUM(yield.units)
      WITH SYNONYMS = ('units produced', 'total units', 'volume'),
    yield.total_scrap AS SUM(yield.scrap_units)
      WITH SYNONYMS = ('scrap', 'total scrap', 'rejects', 'failed units'),
    yield.avg_yield_pct AS AVG(yield.yield_pct)
      WITH SYNONYMS = ('average yield', 'yield percent', 'first pass yield percent'),
    yield.avg_humidity AS AVG(yield.booth_humidity)
      WITH SYNONYMS = ('humidity', 'average booth humidity'),
    defects.defect_count AS SUM(defects.defect_n)
      WITH SYNONYMS = ('defect count', 'number of defects'),
    stations.avg_reading AS AVG(stations.reading_avg)
      WITH SYNONYMS = ('average reading', 'average sensor value'),
    stations.peak_reading AS MAX(stations.reading_max)
      WITH SYNONYMS = ('peak reading', 'max sensor value')
  )
  COMMENT = 'Cascade Cycleworks plant floor: yield, scrap, defects and station telemetry at a 5-minute grain.';


-- =====================================================================
-- CHECKPOINTS -- the three questions the agent has to answer in Parts 4 and 5
-- =====================================================================
-- Query a semantic view with SEMANTIC_VIEW(), naming DIMENSIONS and METRICS.
-- If these three work, the agent has what it needs -- and if the agent later
-- gets one of them wrong, you know the problem is in its instructions, not in
-- this view. That is the point of running them by hand first.

-- Q1  "What is first-pass yield by line right now?"
SELECT * FROM SEMANTIC_VIEW(MFG.ANALYTICS.PLANT_FLOOR_SV
  DIMENSIONS yield.line
  METRICS yield.avg_yield_pct, yield.total_units, yield.total_scrap)
ORDER BY 1;

-- Q2  "Which defect is driving scrap on PAINT?"
SELECT * FROM SEMANTIC_VIEW(MFG.ANALYTICS.PLANT_FLOOR_SV
  DIMENSIONS defects.defect_code
  METRICS defects.defect_count
  WHERE yield.line = 'PAINT' AND defects.defect_code <> 'NONE')
ORDER BY 2 DESC;

-- Q3  "Why did PAINT yield drop?"  <- the payoff, and the reason the second
--     data source exists. Yield and booth humidity in the same result set,
--     bucket by bucket. During the incident you see humidity climb from ~44
--     into the 60s-70s while yield falls from ~99% to the mid 70s.
SELECT * FROM SEMANTIC_VIEW(MFG.ANALYTICS.PLANT_FLOOR_SV
  DIMENSIONS yield.line, yield.bucket
  METRICS yield.avg_yield_pct, yield.avg_humidity
  WHERE yield.line = 'PAINT')
ORDER BY 2 DESC
LIMIT 6;
```
