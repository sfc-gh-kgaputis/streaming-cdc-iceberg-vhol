-- =====================================================================
-- 00_bootstrap.sql   RUN ONCE in Snowsight, as your signup ACCOUNTADMIN
-- =====================================================================
-- Account settings and the lab identity ONLY. Cortex Code cannot create its
-- own login, so this has to run before you connect it.
--
-- Everything else -- database, schemas, warehouse, tables, Dynamic Tables,
-- the semantic view, the agent -- you build by PROMPTING Cortex Code.
-- See the README walkthrough.
--
-- HOW TO RUN: highlight one block and run it (Cmd/Ctrl+Enter).
--   BLOCK 1  account settings
--   BLOCK 2  lab identity + token   -> copy the token into secret.pat
--   BLOCK 3  your account identifier -> paste into Cortex Code
-- =====================================================================


-- ===================== BLOCK 1: account settings =====================
USE ROLE ACCOUNTADMIN;

-- The producer emits UTC event times. Without this, every freshness and
-- per-layer latency measurement in the lab is off by your UTC offset.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- REQUIRED for the agent step. This defaults to DISABLED on a fresh account,
-- which confines inference to your home region and shrinks both the model list
-- and the available Cortex features. The agent will degrade or fail without it.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';


-- ================== BLOCK 2: lab identity + token ====================
-- One identity authenticates BOTH Cortex Code and the data producer, so you
-- only manage one credential. ACCOUNTADMIN so Cortex Code can build the pipeline.
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS HOL_USER
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Iceberg CDC VHOL lab user';
GRANT ROLE ACCOUNTADMIN TO USER HOL_USER;

-- Cortex access is NOT implied by ACCOUNTADMIN -- these are database roles and
-- have to be granted explicitly, or the agent step in Part 4 fails.
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER  TO ROLE ACCOUNTADMIN;
GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER TO ROLE ACCOUNTADMIN;

-- Cortex Agents resolve permissions from the user's DEFAULT role, not the role
-- active in the session. HOL_USER's default is ACCOUNTADMIN above; keep it that
-- way or the agent will silently lose access to the pipeline.

-- Tokens require the user to sit under a network policy. This one is permissive
-- because it is a throwaway lab account; do not copy this into anything real.
CREATE NETWORK POLICY IF NOT EXISTS HOL_NP ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER HOL_USER SET NETWORK_POLICY = HOL_NP;

ALTER USER HOL_USER
  ADD PROGRAMMATIC ACCESS TOKEN HOL_PAT
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 7
    COMMENT = 'Iceberg CDC VHOL lab token';
-- >>> Copy token_secret NOW (it is shown once) into a file named secret.pat
-- >>> in the root of this repo. It is gitignored. <<<

-- NOTE: If you forgot to capture the code on initial creation, 
-- you can use ROTATE to get a new one!
-- ALTER USER HOL_USER
--   ROTATE PROGRAMMATIC ACCESS TOKEN HOL_PAT;

-- ================= BLOCK 3: your account identifier ==================
-- Paste this value into Cortex Code's "Account identifier" field.
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;

-- =========== OPTIONAL: teardown of the identity (run later) ===========
-- Run these from Snowsight as your signup admin, NOT from Cortex Code --
-- Cortex Code is connected AS HOL_USER and cannot drop the user it is
-- authenticated with. Lab objects are dropped in 09_cleanup.sql.
--
-- ALTER USER HOL_USER REMOVE PROGRAMMATIC ACCESS TOKEN HOL_PAT;
-- DROP USER IF EXISTS HOL_USER;
-- DROP NETWORK POLICY IF EXISTS HOL_NP;
-- (List tokens: SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER HOL_USER;)
SELECT 'bootstrap complete' AS status;