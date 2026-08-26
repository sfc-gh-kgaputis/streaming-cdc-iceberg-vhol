-- =====================================================================
-- 06_agent.sql   Answer key for Part 4
-- =====================================================================
-- The Cascade Plant Analyst. A Cortex Agent grounded on the semantic view.
--
-- THINGS THAT WILL COST YOU TIME:
--
--  * Dollar-quote the spec with $$ and NOT a named tag like $spec$. Cortex
--    Code's SQL execution path rejects named dollar-quote tags. The spec JSON
--    never contains $$ itself, so plain $$ is safe.
--
--  * Set models.orchestration to "auto". Agent orchestration has a narrower,
--    account-specific allowed-models list than Cortex COMPLETE does, so a
--    pinned model can fail with "not an allowed model for Agent". Pin it later
--    in Snowsight under Configuration -> Model if you want a specific one.
--
--  * To change the agent, RE-RUN this whole CREATE OR REPLACE statement. Do
--    not try a workspace-file edit/redeploy path -- this agent is created from
--    SQL and is not tracked in a workspace, so that fails with
--    "Could not resolve workspace file ... cortex-project.yaml".
--
--  * ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION' must have
--    run (00_bootstrap.sql, BLOCK 1). Without it the agent degrades or fails.
--
--  * Keep execution_environment in tool_resources. The Analyst tool needs a
--    warehouse to run its generated SQL in, and the spec is accepted without
--    one -- CREATE AGENT succeeds, then every question comes back as
--    "internal error (request_id: ...)", code 391920, which mentions neither
--    the warehouse nor the tool. Adding it is the whole fix.
--
-- WHERE TO CHAT WITH IT: Snowsight -> AI & ML -> Agents -> Cascade Plant Analyst,
-- using the chat panel on the detail page. You do NOT need to Publish -- the
-- agent already exists from this SQL. Publish is only for sharing a UI-edited
-- version.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.ANALYTICS;

CREATE OR REPLACE AGENT MFG.ANALYTICS.CASCADE_PLANT_ANALYST
WITH PROFILE='{"display_name":"Cascade Plant Analyst"}'
FROM SPECIFICATION $$
{
  "models": { "orchestration": "auto" },
  "instructions": {
    "response": "You are the plant-floor analyst for Cascade Cycleworks, a bicycle frame manufacturer. You speak to a plant manager who is standing on the factory floor and needs to decide whether to stop a line. Be brief and concrete. Lead with the number and the line name. Always state the 5-minute interval your numbers come from, in UTC. Never speculate beyond the data you queried; if something is not in the data, say so plainly.",
    "orchestration": "The plant runs three sequential lines: WELD, then PAINT, then ASSEMBLY. Every frame is scanned at the end of each line as PASS or FAIL with an optional defect code.\n\nAlways use the plant_floor tool. Data arrives continuously with roughly a 1-2 minute pipeline lag, so 'right now' means the most recent 5-minute buckets, not the current clock time. Prefer the latest 2-3 buckets and say which you used.\n\nFirst-pass yield is the percentage of scans that PASSED. Healthy is above about 95 percent. Below 90 percent on any line is an active problem worth reporting urgently.\n\nWhen asked WHY yield dropped, do not stop at the defect code. The same table carries AVG_BOOTH_HUMIDITY for the paint booth in the same 5-minute interval. Compare it across recent buckets: booth humidity above about 60 is abnormal (normal is around 44) and is a known cause of PAINT_RUN defects, because a humid booth stops the finish flashing off correctly. Report the correlation and the sequence: humidity rose first, then defects followed. Booth humidity is only measured for PAINT, so it is legitimately empty for WELD and ASSEMBLY.\n\nWhen asked which defect is driving scrap, exclude the DEFECT_CODE value 'NONE' -- that means the scan passed and is not a defect.\n\nYield can legitimately go UP for an interval that already reported: inspectors re-inspect failed frames and overturn them to PASS, which corrects history. If a number changed from a previous answer, that is the data being corrected, not an error."
  },
  "tools": [
    { "tool_spec": { "type": "cortex_analyst_text_to_sql", "name": "plant_floor",
        "description": "Yield, scrap, defect counts and station telemetry for the Cascade Cycleworks plant floor at a 5-minute grain." } }
  ],
  "tool_resources": {
    "plant_floor": {
      "semantic_view": "MFG.ANALYTICS.PLANT_FLOOR_SV",
      "execution_environment": { "type": "warehouse", "warehouse": "HOL_WH" }
    }
  }
}
$$;


-- =====================================================================
-- CHECKPOINT
-- =====================================================================
SHOW AGENTS LIKE 'CASCADE_PLANT_ANALYST' IN SCHEMA MFG.ANALYTICS;

-- Then go to Snowsight -> AI & ML -> Agents -> Cascade Plant Analyst and ask,
-- in this order. The third one is the payoff:
--
--   1. "What is first-pass yield by line right now?"
--   2. "Which defect is driving scrap on PAINT?"
--   3. "Why did PAINT yield drop?"
--
-- Question 3 is the one that matters. Answering it requires the agent to reach
-- across BOTH sources -- the CDC scan feed for yield and the streamed sensor
-- telemetry for booth humidity -- and notice that humidity rose before the
-- defects did. An agent on the CDC feed alone can only tell you WHAT happened.
