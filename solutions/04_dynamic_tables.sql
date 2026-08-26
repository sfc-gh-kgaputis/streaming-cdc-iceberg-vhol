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
--    a schema default is ever missed. A Dynamic Iceberg Table takes its format
--    version from the schema it is created IN -- MFG.ANALYTICS here -- and
--    CREATE DYNAMIC ICEBERG TABLE has no version clause, so that default is the
--    only thing standing between this file and a v2 Gold layer. 02_preflight.sql
--    check 4 is what proves it.
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
USE SCHEMA MFG.ANALYTICS;

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
-- This is the join that pays for the second data source. Yield alone tells you
-- PAINT is scrapping frames; yield next to humidity tells you WHY.
--
-- AVG_BOOTH_HUMIDITY is NULL for WELD and ASSEMBLY, which is correct: booth
-- humidity is a paint-booth metric. The LEFT JOIN keeps those lines in the
-- result instead of dropping them.
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

-- A trailing real statement, deliberately: Snowsight parses text after the last
-- statement as a statement, so a file ENDING in comments throws
-- "SQL compilation error: Empty SQL statement" when you run the whole thing.
SELECT 'dynamic tables created -- all four must read INCREMENTAL' AS status;
