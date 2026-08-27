-- =====================================================================
-- 06_agent.sql   Answer key for Part 4
-- =====================================================================
-- The Cascade Plant Analyst. A Cortex Agent grounded on the semantic view.
--
-- Three things the spec below gets right, so keep them when you edit it:
--   * the specification is dollar-quoted with bare $$
--   * models.orchestration is "auto"
--   * tool_resources carries execution_environment, naming HOL_WH
--
-- To change the agent, re-run this whole CREATE OR REPLACE statement.
--
-- If anything misbehaves, see docs/troubleshooting.md, "Agent".
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

-- Then go to Snowsight -> AI & ML -> Agents -> Cascade Plant Analyst and ask
-- these three in order:
--
--   1. "What is first-pass yield by line right now?"
--   2. "Which defect is driving scrap on PAINT?"
--   3. "Why did PAINT yield drop?"
--
-- Question 3 is the one that matters. Answering it requires the agent to reach
-- across BOTH sources -- the CDC scan feed for yield and the streamed sensor
-- telemetry for booth humidity -- and notice that humidity rose before the
-- defects did. An agent on the CDC feed alone can only tell you WHAT happened.

-- A trailing real statement, deliberately: Snowsight parses text after the last
-- statement as a statement, so a file ENDING in comments throws
-- "SQL compilation error: Empty SQL statement" when you run the whole thing.
SELECT 'agent created -- now chat with it in Snowsight' AS status;
