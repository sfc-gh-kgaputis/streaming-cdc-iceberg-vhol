-- =====================================================================
-- 02_preflight.sql   Run this right after Part 1. Takes 5 seconds.
-- =====================================================================
-- Four checks. All must return TRUE before you go any further. This is the
-- cheapest insurance in the lab: it turns a confusing failure deep in the
-- pipeline into a 30-second fix now.
--
-- Note what this file does NOT do: check SHOW PARAMETERS. A schema can report
-- ICEBERG_VERSION_DEFAULT = 3 at SCHEMA level and still produce v2 tables,
-- because that parameter resolves from the session's current schema rather
-- than from the schema holding the new table. SHOW PARAMETERS is therefore
-- not evidence. The only trustworthy check is the format version of a table
-- you actually created -- which is what checks 3 and 4 do, in both schemas,
-- because passing in one proves nothing about the other.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;

-- 1. Cloud and region. Managed Iceberg storage is AWS/Azure only.
SELECT CURRENT_REGION()                     AS region,
       STARTSWITH(CURRENT_REGION(), 'AWS_') AS aws_ok,
       CURRENT_VERSION()                    AS version;

-- 2. Cortex cross-region inference must not be DISABLED, or the agent step
--    in Part 6 will degrade or fail. Fixed by BLOCK 1 of 00_bootstrap.sql.
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT
  ->> SELECT "value" AS cross_region, "value" <> 'DISABLED' AS cortex_ok FROM $1;

-- 3. Managed Iceberg v3 really resolves in MFG.CDC.
--    The CREATE deliberately specifies no catalog, volume, or version -- that is
--    the point. The VARIANT column is the actual test: v2 rejects VARIANT
--    outright, so if this statement succeeds AND reports version 3, the whole
--    inheritance chain is working.
USE SCHEMA MFG.CDC;
CREATE OR REPLACE ICEBERG TABLE MFG.CDC._PREFLIGHT (N NUMBER(38,0), M VARIANT);

SHOW ICEBERG TABLES LIKE '_PREFLIGHT' IN SCHEMA MFG.CDC
  ->> SELECT 'MFG.CDC'                        AS schema_,
             "external_volume_name"           AS volume,
             "iceberg_table_type"             AS kind,
             "iceberg_table_format_version"   AS format_version,
             "external_volume_name" = 'SNOWFLAKE_MANAGED'
               AND "iceberg_table_format_version" = 3 AS cdc_iceberg_ok
      FROM $1;

DROP TABLE MFG.CDC._PREFLIGHT;

-- 4. Same again in MFG.RAW, where the telemetry table lives.
USE SCHEMA MFG.RAW;
CREATE OR REPLACE ICEBERG TABLE MFG.RAW._PREFLIGHT (N NUMBER(38,0), M VARIANT);

SHOW ICEBERG TABLES LIKE '_PREFLIGHT' IN SCHEMA MFG.RAW
  ->> SELECT 'MFG.RAW'                        AS schema_,
             "external_volume_name"           AS volume,
             "iceberg_table_type"             AS kind,
             "iceberg_table_format_version"   AS format_version,
             "external_volume_name" = 'SNOWFLAKE_MANAGED'
               AND "iceberg_table_format_version" = 3 AS raw_iceberg_ok
      FROM $1;

DROP TABLE MFG.RAW._PREFLIGHT;

USE SCHEMA MFG.CDC;

-- Belt and braces: confirm the tables you already built are actually v3.
-- If STATION_TELEMETRY came out v2, recreate it -- Iceberg has no in-place
-- v2 -> v3 upgrade.
SHOW ICEBERG TABLES IN DATABASE MFG
  ->> SELECT "name", "iceberg_table_format_version" AS format_version,
             "iceberg_table_format_version" = 3 AS is_v3
      FROM $1 ORDER BY "name";

-- =====================================================================
-- aws_ok, cortex_ok, cdc_iceberg_ok and raw_iceberg_ok must ALL be TRUE,
-- and every row of the last query must show is_v3 = TRUE.
--
-- If either *_iceberg_ok is FALSE, or a CREATE failed with
-- "Unsupported data type 'VARIANT' for iceberg tables":
--   Your session's current schema was not one that resolves
--   ICEBERG_VERSION_DEFAULT = 3. Re-run 01_environment.sql (it sets the
--   database-level default and issues USE SCHEMA before each create), then
--   recreate any table that came back v2 and re-run this file.
--
-- If cortex_ok is FALSE: re-run BLOCK 1 of 00_bootstrap.sql as ACCOUNTADMIN
-- in Snowsight.
-- =====================================================================
