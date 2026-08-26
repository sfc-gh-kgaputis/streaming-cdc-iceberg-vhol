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
