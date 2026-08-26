-- =====================================================================
-- 02_preflight.sql   Run this right after Part 1. Takes 5 seconds.
-- =====================================================================
-- Three checks. All three must return TRUE before you go any further.
-- This is the cheapest insurance in the lab: it turns a confusing failure
-- deep in the pipeline into a 30-second fix now.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;

-- 1. Cloud and region. Managed Iceberg storage is AWS/Azure only.
SELECT CURRENT_REGION()                     AS region,
       STARTSWITH(CURRENT_REGION(), 'AWS_') AS aws_ok,
       CURRENT_VERSION()                    AS version;

-- 2. Cortex cross-region inference must not be DISABLED, or the agent step
--    in Part 5 will degrade or fail. Fixed by BLOCK 1 of 00_bootstrap.sql.
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT
  ->> SELECT "value" AS cross_region, "value" <> 'DISABLED' AS cortex_ok FROM $1;

-- 3. Managed Iceberg v3 actually resolves in this schema.
--    This CREATE deliberately specifies NO catalog, volume, or version -- that
--    is the entire point. If the three ALTER SCHEMA defaults took, it comes
--    back SNOWFLAKE_MANAGED / MANAGED / 3 on its own.
CREATE OR REPLACE ICEBERG TABLE MFG.CDC._PREFLIGHT (N NUMBER(38,0));

SHOW ICEBERG TABLES LIKE '_PREFLIGHT' IN SCHEMA MFG.CDC
  ->> SELECT "external_volume_name"            AS volume,
             "iceberg_table_type"              AS kind,
             "iceberg_table_format_version"    AS format_version,
             "external_volume_name" = 'SNOWFLAKE_MANAGED'
               AND "iceberg_table_format_version" = 3 AS iceberg_ok
      FROM $1;

DROP TABLE MFG.CDC._PREFLIGHT;

-- aws_ok, cortex_ok and iceberg_ok must ALL be TRUE.
--
-- If iceberg_ok is FALSE: you missed one of the three ALTER SCHEMA statements
-- in 01_environment.sql. Re-run them, then re-run this file.
--
-- If cortex_ok is FALSE: re-run BLOCK 1 of 00_bootstrap.sql as ACCOUNTADMIN.
