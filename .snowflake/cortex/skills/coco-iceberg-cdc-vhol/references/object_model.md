# Object model — verbatim DDL

Emit these **exactly**. They are the same statements as the matching
`solutions/*.sql` files in this repo, which is the answer key the attendee can
also read directly. If a `CREATE` fails, re-emit from here rather than
improvising syntax.

Order: environment and tables, then the Dynamic Table DAG, then the semantic view.

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
-- The three ALTER SCHEMA lines are the most load-bearing statements in the
-- whole lab. They make every Iceberg table in the schema resolve to
-- Snowflake-managed storage at format version 3, WITHOUT any table-level
-- clauses. Skip them and tables either fail to create or silently land on
-- v2, which then produces confusing timestamp-scale errors much deeper in
-- the pipeline. CREATE DYNAMIC ICEBERG TABLE has no version clause at all,
-- so it can only inherit these.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS MFG;
CREATE SCHEMA   IF NOT EXISTS MFG.CDC;   -- CDC destination + the pipeline
CREATE SCHEMA   IF NOT EXISTS MFG.RAW;   -- streaming telemetry landing zone

-- Iceberg defaults. Do this BEFORE creating any table.
ALTER SCHEMA MFG.CDC SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.CDC SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.CDC SET ICEBERG_VERSION_DEFAULT = 3;

ALTER SCHEMA MFG.RAW SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.RAW SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.RAW SET ICEBERG_VERSION_DEFAULT = 3;

CREATE WAREHOUSE IF NOT EXISTS HOL_WH
  WAREHOUSE_SIZE = 'XSMALL'
  GENERATION = '2'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.CDC;

-- ---------------------------------------------------------------------
-- The CDC destination table.
--
-- This is a STANDARD table, and that is deliberate: it takes UPDATEs and
-- DELETEs continuously, which is the whole point of a change feed. The
-- _SNOWFLAKE_* columns are what the Openflow connector maintains for you.
-- _SNOWFLAKE_DELETED is a SOFT delete -- the connector never removes rows,
-- it flags them, so history survives. Filtering it is your job downstream.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE MFG.CDC.PRODUCTION_SCANS (
  SCAN_ID                 STRING,          -- replication key (the Postgres PK)
  FRAME_ID                STRING,          -- 'F-000123'
  LINE                    STRING,          -- WELD | PAINT | ASSEMBLY
  SKU                     STRING,
  STATUS                  STRING,          -- PASS | FAIL
  DEFECT_CODE             STRING,          -- NULL on PASS
  STATION_ID              STRING,          -- joins to telemetry
  OPERATOR_ID             STRING,
  EVENT_TS                TIMESTAMP_NTZ,
  UPDATED_TS              TIMESTAMP_NTZ,   -- source-system modification time
  _SNOWFLAKE_INSERTED_AT  TIMESTAMP_NTZ,   -- connector-maintained
  _SNOWFLAKE_UPDATED_AT   TIMESTAMP_NTZ,   -- connector-maintained
  _SNOWFLAKE_DELETED      BOOLEAN          -- connector-maintained soft delete
);

-- ---------------------------------------------------------------------
-- The streaming telemetry table: Iceberg, append-only.
--
-- Note what is NOT here: no CATALOG, no EXTERNAL_VOLUME, no ICEBERG_VERSION.
-- All three come from the schema defaults set above. Verify with 02_preflight.sql.
--
-- Snowpipe Streaming will auto-create a default pipe named
-- STATION_TELEMETRY-STREAMING for this table. You never write CREATE PIPE.
-- ---------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE MFG.RAW.STATION_TELEMETRY (
  STATION_ID  STRING,
  LINE        STRING,
  METRIC      STRING,          -- weld_current | booth_humidity | booth_temp | torque_nm
  VALUE       DOUBLE,
  EVENT_TS    TIMESTAMP_NTZ
);
```

---

## 2. Dynamic Iceberg Table DAG

Source of truth: `solutions/03_dynamic_tables.sql`

```sql
-- =====================================================================
-- 03_dynamic_tables.sql   Answer key for Parts 3 and 4
-- =====================================================================
-- The pipeline. Four Dynamic ICEBERG Tables, every one of them refreshing
-- INCREMENTALLY -- Snowflake recomputes only the groups that changed, not
-- the whole table, even though the CDC source underneath is being UPDATEd
-- and DELETEd continuously.
--
--   PRODUCTION_SCANS (standard, mutating)   STATION_TELEMETRY (Iceberg, append)
--             |                                          |
--             v                                          v
--     DT_SCANS_ACTIVE                          DT_STATION_HEALTH
--     (soft deletes filtered)                  (5-min metric rollup)
--             |                                          |
--             +---------------------+--------------------+
--                                   v
--                      DT_YIELD_BY_LINE_5MIN     <- the two-source join
--                                   |
--                                   v
--                      DT_DEFECT_COUNTS_5MIN
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
USE SCHEMA MFG.CDC;

-- ---------------------------------------------------------------------
-- Layer 1a: the soft-delete filter.
--
-- The connector never hard-deletes. A voided scan is still physically present
-- with _SNOWFLAKE_DELETED = TRUE. If you skip this WHERE clause, voided frames
-- keep counting against yield forever. This one predicate is the difference
-- between a correct CDC pipeline and a plausible-looking wrong one.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.CDC.DT_SCANS_ACTIVE
  TARGET_LAG = '1 minute'
  WAREHOUSE = HOL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT SCAN_ID, FRAME_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID, OPERATOR_ID,
       EVENT_TS, UPDATED_TS,
       IFF(STATUS = 'FAIL', 1, 0) AS IS_SCRAP
FROM MFG.CDC.PRODUCTION_SCANS
WHERE NOT _SNOWFLAKE_DELETED;

-- ---------------------------------------------------------------------
-- Layer 1b: telemetry rolled up to the SAME 5-minute grain as yield, which
-- is what makes the join in the next layer possible.
-- ---------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.CDC.DT_STATION_HEALTH
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
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.CDC.DT_YIELD_BY_LINE_5MIN
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
FROM MFG.CDC.DT_SCANS_ACTIVE s
LEFT JOIN MFG.CDC.DT_STATION_HEALTH h
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
CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.CDC.DT_DEFECT_COUNTS_5MIN
  TARGET_LAG = '1 minute'
  WAREHOUSE = HOL_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT LINE,
       TIME_SLICE(EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6) AS BUCKET,
       COALESCE(DEFECT_CODE, 'NONE')                       AS DEFECT_CODE,
       COUNT(*)                                            AS N
FROM MFG.CDC.DT_SCANS_ACTIVE
GROUP BY 1, 2, 3;


-- =====================================================================
-- CHECKPOINTS
-- =====================================================================

-- Every row must read INCREMENTAL / true, and DOWNGRADE_REASON must be empty.
-- If refresh_mode came back FULL, something in the query blocked incremental
-- refresh and refresh_mode_reason will say what.
SHOW DYNAMIC TABLES IN SCHEMA MFG.CDC
  ->> SELECT "name", "refresh_mode", "is_iceberg", "target_lag", "scheduling_state",
             NULLIF("refresh_mode_reason", '') AS downgrade_reason
      FROM $1 ORDER BY "name";

-- Yield by line, most recent buckets. During the incident PAINT falls well
-- below the other two lines and AVG_BOOTH_HUMIDITY climbs in the same bucket.
SELECT LINE, BUCKET, UNITS, SCRAP_UNITS, FIRST_PASS_YIELD_PCT,
       ROUND(AVG_BOOTH_HUMIDITY, 1) AS HUMIDITY
FROM MFG.CDC.DT_YIELD_BY_LINE_5MIN
ORDER BY BUCKET DESC, LINE
LIMIT 12;

-- Top defect, derived at read time (the MODE() replacement).
SELECT LINE, DEFECT_CODE, SUM(N) AS N
FROM MFG.CDC.DT_DEFECT_COUNTS_5MIN
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
        NAME_PREFIX => 'MFG.CDC.DT_'))
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;


-- =====================================================================
-- THE NEGATIVE EXAMPLE (Part 4) -- run it and read the error
-- =====================================================================
-- This is a real, instructive failure, not a contrived one. It is the first
-- thing most people reach for.
--
-- CREATE OR REPLACE DYNAMIC ICEBERG TABLE MFG.CDC.DT_TOP_DEFECT_BROKEN
--   TARGET_LAG = '1 minute' WAREHOUSE = HOL_WH REFRESH_MODE = INCREMENTAL
-- AS
-- SELECT LINE,
--        TIME_SLICE(EVENT_TS, 5, 'MINUTE')::TIMESTAMP_NTZ(6) AS BUCKET,
--        MODE(DEFECT_CODE) AS TOP_DEFECT
-- FROM MFG.CDC.DT_SCANS_ACTIVE
-- GROUP BY 1, 2;
--
-- Expected:
--   Change tracking is not supported on queries containing the function 'MODE'
```

---

## 3. Semantic view

Source of truth: `solutions/04_semantic_view.sql`

**Syntax rules — all four have been generated wrong before:**
1. Clause order is fixed: `TABLES` → `RELATIONSHIPS` → `FACTS` → `DIMENSIONS` → `METRICS`
2. Tables use `AS`, never `=`
3. Synonyms use `WITH SYNONYMS = (...)`, never a bare `SYNONYMS = (...)`
4. Metrics are alias-qualified and defined with `AS`:
   `yield.total_units AS SUM(yield.units)` — correct.
   `total_units = SUM(units)` — will not compile.

```sql
-- =====================================================================
-- 04_semantic_view.sql   Answer key for Part 5
-- =====================================================================
-- The semantic view is what turns the Gold tables into something an agent can
-- reason about: business names, synonyms, and the metric definitions, so the
-- agent is not guessing at what a column means.
--
-- SYNTAX RULES. Cortex Code has historically generated all four of these wrong.
-- If a CREATE fails, re-emit this DDL verbatim rather than improvising:
--   1. Clause order is fixed: TABLES -> RELATIONSHIPS -> FACTS -> DIMENSIONS -> METRICS
--   2. Tables use AS, never '=':          yield AS MFG.CDC.DT_...
--   3. Synonyms use WITH SYNONYMS = (...), never a bare SYNONYMS = (...)
--   4. Metrics are alias-qualified and defined with AS:
--        yield.total_units AS SUM(yield.units)      -- correct
--        total_units = SUM(units)                   -- WRONG, will not compile
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.CDC;

CREATE OR REPLACE SEMANTIC VIEW MFG.CDC.PLANT_FLOOR_SV
  TABLES (
    yield AS MFG.CDC.DT_YIELD_BY_LINE_5MIN
      PRIMARY KEY (LINE, BUCKET)
      WITH SYNONYMS = ('yield', 'first pass yield', 'production yield', 'line output')
      COMMENT = 'Units, scrap and first-pass yield per production line per 5 minutes, with the paint booth humidity for the same interval.',
    defects AS MFG.CDC.DT_DEFECT_COUNTS_5MIN
      PRIMARY KEY (LINE, BUCKET, DEFECT_CODE)
      WITH SYNONYMS = ('defects', 'defect counts', 'scrap reasons', 'failure codes')
      COMMENT = 'Count of scans per defect code per line per 5 minutes. DEFECT_CODE = NONE means the scan passed.',
    stations AS MFG.CDC.DT_STATION_HEALTH
      PRIMARY KEY (STATION_ID, METRIC, BUCKET)
      WITH SYNONYMS = ('stations', 'station health', 'sensors', 'telemetry', 'machine metrics')
      COMMENT = 'Sensor telemetry averaged per station per metric per 5 minutes.'
  )
  RELATIONSHIPS (
    defects_to_yield AS defects (LINE, BUCKET) REFERENCES yield (LINE, BUCKET),
    stations_to_yield AS stations (LINE, BUCKET) REFERENCES yield (LINE, BUCKET)
  )
  FACTS (
    yield.units AS UNITS,
    yield.scrap_units AS SCRAP_UNITS,
    yield.yield_pct AS FIRST_PASS_YIELD_PCT,
    yield.booth_humidity AS AVG_BOOTH_HUMIDITY,
    defects.defect_n AS N,
    stations.reading_avg AS AVG_VALUE,
    stations.reading_max AS MAX_VALUE
  )
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
-- CHECKPOINTS -- the three questions the agent has to answer in Part 6
-- =====================================================================
-- Query a semantic view with SEMANTIC_VIEW(), naming DIMENSIONS and METRICS.
-- If these three work, the agent has what it needs.

-- Q1  "What is first-pass yield by line right now?"
SELECT * FROM SEMANTIC_VIEW(MFG.CDC.PLANT_FLOOR_SV
  DIMENSIONS yield.line
  METRICS yield.avg_yield_pct, yield.total_units, yield.total_scrap)
ORDER BY 1;

-- Q2  "Which defect is driving scrap on PAINT?"
SELECT * FROM SEMANTIC_VIEW(MFG.CDC.PLANT_FLOOR_SV
  DIMENSIONS defects.defect_code
  METRICS defects.defect_count
  WHERE yield.line = 'PAINT' AND defects.defect_code <> 'NONE')
ORDER BY 2 DESC;

-- Q3  "Why did PAINT yield drop?"  <- the payoff, and the reason the second
--     data source exists. Yield and booth humidity in the same result set,
--     bucket by bucket. During the incident you see humidity climb from ~44
--     into the 60s-70s while yield falls from ~99% to the mid 70s.
SELECT * FROM SEMANTIC_VIEW(MFG.CDC.PLANT_FLOOR_SV
  DIMENSIONS yield.line, yield.bucket
  METRICS yield.avg_yield_pct, yield.avg_humidity
  WHERE yield.line = 'PAINT')
ORDER BY 2 DESC
LIMIT 6;
```
