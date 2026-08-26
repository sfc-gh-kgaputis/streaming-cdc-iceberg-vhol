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
-- The CDC destination table.
--
-- This is a STANDARD table, and that is deliberate: it takes UPDATEs and
-- DELETEs continuously, which is the whole point of a change feed. The
-- _SNOWFLAKE_* columns are what the Openflow connector maintains for you.
-- _SNOWFLAKE_DELETED is a SOFT delete -- the connector never removes rows,
-- it flags them, so history survives. Filtering it is your job downstream.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE MFG.RAW.QUALITY_INSPECTIONS (
  INSPECTION_ID           STRING,          -- replication key (the Postgres PK)
  UNIT_ID                 STRING,          -- 'F-000123'
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
