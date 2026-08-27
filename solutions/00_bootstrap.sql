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
--   BLOCK 2  lab identity + token   -> paste the token into profile.json
--   BLOCK 3  your account identifier -> paste into Cortex Code
-- =====================================================================


-- ===================== BLOCK 1: account settings =====================
USE ROLE ACCOUNTADMIN;

-- Set the account to UTC. The producer emits UTC event times, so this is what
-- makes every freshness and per-layer latency measurement in the lab read right.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- REQUIRED for the agent step. Set it now: a fresh account defaults to DISABLED,
-- which confines inference to your home region and shrinks both the model list and
-- the available Cortex features.
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';


-- ================== BLOCK 2: lab identity + token ====================
-- One identity authenticates BOTH Cortex Code and the data producer, so you
-- only manage one credential. ACCOUNTADMIN so Cortex Code can build the pipeline.
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS HOL_USER
  DEFAULT_ROLE = ACCOUNTADMIN
  COMMENT = 'Iceberg CDC VHOL lab user';
GRANT ROLE ACCOUNTADMIN TO USER HOL_USER;

-- Grant Cortex access explicitly. These are database roles, so ACCOUNTADMIN does
-- not imply them, and the agent step in Part 4 needs them.
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER  TO ROLE ACCOUNTADMIN;
GRANT DATABASE ROLE SNOWFLAKE.COPILOT_USER TO ROLE ACCOUNTADMIN;

-- Leave HOL_USER's DEFAULT_ROLE as ACCOUNTADMIN, set above. Cortex Agents resolve
-- permissions from the user's default role, not the role active in the session.

-- Attach a network policy before minting the token: a token only authenticates if
-- its user sits under one. This policy is permissive because it is a throwaway lab
-- account; do not copy it into anything real.
CREATE NETWORK POLICY IF NOT EXISTS HOL_NP ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER HOL_USER SET NETWORK_POLICY = HOL_NP;

ALTER USER HOL_USER
  ADD PROGRAMMATIC ACCESS TOKEN HOL_PAT
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = 7
    COMMENT = 'Iceberg CDC VHOL lab token';
-- >>> Copy token_secret NOW (it is shown once). Copy profile.example.json to
-- >>> profile.json in the root of this repo and paste it in as
-- >>> personal_access_token. profile.json is gitignored. <<<
-- If you miss it, docs/troubleshooting.md has the ROTATE statement.

-- ================= BLOCK 3: your account identifier ==================
-- Paste this value into Cortex Code's "Account identifier" field.
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;

SELECT 'bootstrap complete -- paste the token and account into profile.json, then Setup C' AS status;
