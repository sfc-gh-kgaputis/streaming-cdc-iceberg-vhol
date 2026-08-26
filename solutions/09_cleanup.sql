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
ALTER DYNAMIC TABLE MFG.ANALYTICS.DEFECT_COUNTS_5MIN SUSPEND;
ALTER DYNAMIC TABLE MFG.ANALYTICS.YIELD_BY_LINE_5MIN SUSPEND;
ALTER DYNAMIC TABLE MFG.ANALYTICS.STATION_HEALTH       SUSPEND;
ALTER DYNAMIC TABLE MFG.ANALYTICS.INSPECTIONS_ACTIVE   SUSPEND;

-- There is no CDC merge task to suspend: the producer issues the MERGE itself.
-- Stopping the producer (Ctrl-C) stops the merges.

ALTER WAREHOUSE HOL_WH SUSPEND;

-- Confirm nothing is still scheduled to run.
SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS
  ->> SELECT "name", "scheduling_state" FROM $1 ORDER BY "name";


-- ====================== BLOCK 2: remove everything ======================
-- Also stop the producer if it is still running (Ctrl-C in its terminal).
DROP AGENT         IF EXISTS MFG.ANALYTICS.CASCADE_PLANT_ANALYST;
DROP SEMANTIC VIEW IF EXISTS MFG.ANALYTICS.PLANT_FLOOR_SV;

-- Presenter only: the live dashboard, if you deployed it. Attendees will not
-- have this -- it is not a lab step, so IF EXISTS does the right thing.
DROP STREAMLIT IF EXISTS MFG.ANALYTICS.PLANT_FLOOR_LIVE;

DROP DYNAMIC TABLE IF EXISTS MFG.ANALYTICS.DEFECT_COUNTS_5MIN;
DROP DYNAMIC TABLE IF EXISTS MFG.ANALYTICS.YIELD_BY_LINE_5MIN;
DROP DYNAMIC TABLE IF EXISTS MFG.ANALYTICS.STATION_HEALTH;
DROP DYNAMIC TABLE IF EXISTS MFG.ANALYTICS.INSPECTIONS_ACTIVE;

-- CDC path: the stream, then the journal it reads.
DROP STREAM IF EXISTS MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM;
DROP TABLE  IF EXISTS MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1;

DROP TABLE IF EXISTS MFG.RAW.SIMULATOR_CONTROL;
DROP TABLE IF EXISTS MFG.RAW.STATION_TELEMETRY;
DROP TABLE IF EXISTS MFG.RAW.QUALITY_INSPECTIONS;

DROP DATABASE  IF EXISTS MFG;
DROP WAREHOUSE IF EXISTS HOL_WH;

-- ============= BLOCK 3: the lab identity (Snowsight only) =============
-- Run these from Snowsight as your SIGNUP admin, not from Cortex Code -- Cortex
-- Code is connected AS HOL_USER and cannot drop the user it is authenticated as.
-- Uncomment to run.
--
-- ALTER USER HOL_USER REMOVE PROGRAMMATIC ACCESS TOKEN HOL_PAT;
-- DROP USER IF EXISTS HOL_USER;
-- DROP NETWORK POLICY IF EXISTS HOL_NP;
-- (List tokens: SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER HOL_USER;)

-- Finally, delete your local secret.pat and producer/profile.json.

-- A trailing real statement, deliberately: Snowsight parses text after the last
-- statement as a statement, so a file ENDING in comments throws
-- "SQL compilation error: Empty SQL statement" when you run the whole thing.
SELECT 'cleanup complete' AS status;
