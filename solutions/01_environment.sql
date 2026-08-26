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
-- So `CREATE ICEBERG TABLE MFG.CDC.T (...)` run without `USE SCHEMA MFG.CDC`
-- first gets the right volume and catalog but lands on **version 2**, even
-- though MFG.CDC has the version default set. SHOW PARAMETERS will cheerfully
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
CREATE SCHEMA   IF NOT EXISTS MFG.CDC;   -- CDC destination + the pipeline
CREATE SCHEMA   IF NOT EXISTS MFG.RAW;   -- streaming telemetry landing zone

-- Layer 2: database level. Any session whose current schema is anywhere inside
-- MFG now inherits v3, which covers the case where you are in MFG.RAW and
-- create something in MFG.CDC.
ALTER DATABASE MFG SET ICEBERG_VERSION_DEFAULT = 3;

-- Storage defaults. Do this BEFORE creating any table.
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

USE SCHEMA MFG.CDC;
