-- =====================================================================
-- 01_environment.sql   Answer key for Part 1
-- =====================================================================
-- You build this by PROMPTING Cortex Code. This file is what it should
-- produce. Use it to check your work, or to catch up if you fall behind.
--
-- The storage defaults below make Iceberg tables resolve to Snowflake-managed
-- storage at format version 3 with no table-level clauses. Set them before
-- creating any table.
--
-- READ THIS, IT IS NOT WHAT YOU EXPECT (measured 26 Aug 2026):
--
--   EXTERNAL_VOLUME and CATALOG always resolve from the schema that CONTAINS the
--   new table. ICEBERG_VERSION_DEFAULT does not, and it resolves differently for
--   the two CREATE forms:
--
--     CREATE ICEBERG TABLE          -> from the SESSION'S CURRENT SCHEMA
--     CREATE DYNAMIC ICEBERG TABLE  -> from the TARGET SCHEMA
--
-- So `CREATE ICEBERG TABLE MFG.RAW.T (...)` run without `USE SCHEMA MFG.RAW`
-- first gets the right volume and catalog but lands on **version 2**, even
-- though MFG.RAW has the version default set. SHOW PARAMETERS reports
-- `value = 3, level = SCHEMA` the whole time, so never use it as proof that v3
-- is working. Only a created table's iceberg_table_format_version counts.
--
-- A v2 table is created successfully; the damage shows up later as
-- `Unsupported data type 'VARIANT'` or a rejected TIMESTAMP_NTZ(9) from
-- TIME_SLICE(), deep in the pipeline where the cause is invisible. So:
-- USE SCHEMA before every plain Iceberg create, and set the defaults on BOTH
-- schemas. 02_preflight.sql checks the result rather than trusting it.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS MFG;
CREATE SCHEMA   IF NOT EXISTS MFG.RAW;        -- both landing zones: CDC destination, its journal, telemetry
CREATE SCHEMA   IF NOT EXISTS MFG.ANALYTICS;  -- everything derived: the Dynamic Tables, semantic view, agent

-- Belt and braces at database level, so any session whose current schema is
-- anywhere inside MFG resolves v3 for a plain Iceberg create.
ALTER DATABASE MFG SET ICEBERG_VERSION_DEFAULT = 3;

-- Storage defaults. Do this BEFORE creating any table.
ALTER SCHEMA MFG.RAW SET EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED';
ALTER SCHEMA MFG.RAW SET CATALOG = 'SNOWFLAKE';
ALTER SCHEMA MFG.RAW SET ICEBERG_VERSION_DEFAULT = 3;

-- ANALYTICS needs these too, and it is easy to think it does not, because you
-- never create a plain Iceberg table there. Every object in it is a Dynamic
-- ICEBERG Table, those read the TARGET schema's version default, and
-- CREATE DYNAMIC ICEBERG TABLE has no ICEBERG_VERSION clause to override with.
-- This line is the only thing deciding whether the Gold layer is v3.
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
-- MFG.RAW.QUALITY_INSPECTIONS, the change journal and the journal stream belong
-- to the connector, and it creates all three itself on first run -- see
-- producer/cdc_simulator.py, ensure_objects().
--
-- What stays here is what the connector does not own: the database, the schemas
-- and their Iceberg defaults, the warehouse, the telemetry table you stream
-- into directly, and the simulator's control plane.
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
-- The simulator's control plane.
--
-- This is how you change the WORLD in Part 5 without touching the pipeline.
-- You write a row here; the running producer notices within ~10 seconds and
-- the plant floor starts behaving differently. Streaming never stops, which is
-- how a real connector behaves: an incident changes the data at the source.
--
-- A STANDARD table, not Iceberg: it is operational metadata, not a feed.
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
