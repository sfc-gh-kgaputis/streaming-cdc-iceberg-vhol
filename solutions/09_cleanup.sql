-- =====================================================================
-- 09_cleanup.sql   RUN THIS WHEN YOU ARE DONE
-- =====================================================================
-- Not optional housekeeping. The Dynamic Tables in this lab refresh on a
-- 1-minute target lag, which keeps a warehouse waking up every minute for as
-- long as they exist. Left running, they will quietly consume trial credits
-- for days after the session ends.
--
-- Run BLOCK 1 to stop the spend. Run BLOCK 2 to remove everything.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- ================== BLOCK 1: stop the refreshes (keep the data) ==================
-- Do this first if you want to keep looking at what you built without paying
-- for continuous refreshes. Suspend the leaves before the roots.
ALTER DYNAMIC TABLE MFG.CDC.DT_DEFECT_COUNTS_5MIN SUSPEND;
ALTER DYNAMIC TABLE MFG.CDC.DT_YIELD_BY_LINE_5MIN SUSPEND;
ALTER DYNAMIC TABLE MFG.CDC.DT_STATION_HEALTH     SUSPEND;
ALTER DYNAMIC TABLE MFG.CDC.DT_SCANS_ACTIVE       SUSPEND;

-- There is no CDC merge task to suspend: the producer issues the MERGE itself.
-- Stopping the producer (Ctrl-C) stops the merges.

ALTER WAREHOUSE HOL_WH SUSPEND;

-- Confirm nothing is still scheduled to run.
SHOW DYNAMIC TABLES IN SCHEMA MFG.CDC
  ->> SELECT "name", "scheduling_state" FROM $1 ORDER BY "name";


-- ====================== BLOCK 2: remove everything ======================
-- Also stop the producer if it is still running (Ctrl-C in its terminal).
DROP AGENT         IF EXISTS MFG.CDC.CASCADE_PLANT_ANALYST;
DROP SEMANTIC VIEW IF EXISTS MFG.CDC.PLANT_FLOOR_SV;

DROP DYNAMIC TABLE IF EXISTS MFG.CDC.DT_DEFECT_COUNTS_5MIN;
DROP DYNAMIC TABLE IF EXISTS MFG.CDC.DT_YIELD_BY_LINE_5MIN;
DROP DYNAMIC TABLE IF EXISTS MFG.CDC.DT_STATION_HEALTH;
DROP DYNAMIC TABLE IF EXISTS MFG.CDC.DT_SCANS_ACTIVE;

-- CDC path: the stream, then the journal it reads.
DROP STREAM IF EXISTS MFG.CDC.PRODUCTION_SCANS_JOURNAL_1787700000_1_STREAM;
DROP TABLE  IF EXISTS MFG.CDC.PRODUCTION_SCANS_JOURNAL_1787700000_1;

DROP TABLE IF EXISTS MFG.RAW.STATION_TELEMETRY;
DROP TABLE IF EXISTS MFG.CDC.PRODUCTION_SCANS;

DROP DATABASE  IF EXISTS MFG;
DROP WAREHOUSE IF EXISTS HOL_WH;

-- The lab identity is dropped from 00_bootstrap.sql instead -- Cortex Code is
-- connected AS VHOLuser and cannot drop the user it is authenticated with.
-- See the teardown block at the bottom of that file, and run it from Snowsight.

-- Finally, delete your local secret.pat and producer/profile.json.
