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

CREATE USER IF NOT EXISTS VHOLuser
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Iceberg CDC VHOL lab user';
GRANT ROLE ACCOUNTADMIN TO USER VHOLuser;

-- Tokens require the user to sit under a network policy. This one is permissive
-- because it is a throwaway lab account; do not copy this into anything real.
CREATE NETWORK POLICY IF NOT EXISTS vhol_np ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER VHOLuser SET NETWORK_POLICY = vhol_np;

ALTER USER VHOLuser
  ADD PROGRAMMATIC ACCESS TOKEN vhol_pat
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 7
    COMMENT = 'Iceberg CDC VHOL lab token';
-- >>> Copy token_secret NOW (it is shown once) into a file named secret.pat
-- >>> in the root of this repo. It is gitignored. <<<


-- ================= BLOCK 3: your account identifier ==================
-- Paste this value into Cortex Code's "Account identifier" field.
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;


-- =========== OPTIONAL: teardown of the identity (run later) ===========
-- Run these from Snowsight as your signup admin, NOT from Cortex Code --
-- Cortex Code is connected AS VHOLuser and cannot drop the user it is
-- authenticated with. Lab objects are dropped in 09_cleanup.sql.
--
-- ALTER USER VHOLuser REMOVE PROGRAMMATIC ACCESS TOKEN vhol_pat;
-- DROP USER IF EXISTS VHOLuser;
-- DROP NETWORK POLICY IF EXISTS vhol_np;
-- (List tokens: SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER VHOLuser;)
