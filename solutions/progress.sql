-- =====================================================================
-- progress.sql   "You are here" -- run this any time, in any Part
-- =====================================================================
-- One query that answers three questions at once:
--
--   * attendee: what have I built, and what is still missing?
--   * attendee: is data actually flowing, or am I looking at empty tables?
--   * presenter: who in the room is stuck, and where?
--
-- Safe to run at any point. It reads metadata only and creates nothing.
--
-- WHY row_count AND NOT COUNT(*):  INFORMATION_SCHEMA.TABLES carries a
-- maintained row_count, so this stays one cheap query no matter how many
-- objects exist. It can lag a few seconds behind a live stream -- which is
-- fine here, because the question is "is this climbing", not "what is the
-- exact number". Use the checkpoint queries in the other solution files
-- when you need an exact count.
--
-- WHY THE JOURNAL IS MATCHED BY PREFIX:  the real Openflow CDC connector
-- suffixes its journal with "<epoch-at-registration>_<generation>", and the
-- generation increments on every source schema change. This lab PINS that
-- suffix to 1787700000_1 on purpose, so every attendee has the same object
-- name and the skill can reference it -- see the comment on JOURNAL_SERIES in
-- producer/producer.py. Matching by prefix anyway, because that is the habit
-- that survives contact with a real connector, where it is not predictable.
-- It is also why you build Dynamic Tables on the destination table and never
-- on the journal.
--
-- THE AGENT IS NOT LISTED. Agents have no INFORMATION_SCHEMA view, so it
-- would cost a second statement. Its checkpoint is its own:
--     SHOW AGENTS LIKE 'CASCADE_PLANT_ANALYST' IN SCHEMA MFG.ANALYTICS;
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.ANALYTICS;

WITH expected AS (
    SELECT * FROM VALUES
      (1, 'Part 1', 'RAW',       'QUALITY_INSPECTIONS',         'table',           'CDC destination, maintained by the MERGE'),
      (2, 'Part 1', 'RAW',       'STATION_TELEMETRY',           'iceberg table',   'telemetry, straight from Snowpipe Streaming'),
      (3, 'Part 1', 'RAW',       'QUALITY_INSPECTIONS_JOURNAL', 'iceberg journal', 'change events, connector-internal'),
      (4, 'Part 3', 'ANALYTICS', 'INSPECTIONS_ACTIVE',          'dynamic table',   'soft deletes filtered out'),
      (5, 'Part 3', 'ANALYTICS', 'STATION_HEALTH',              'dynamic table',   'telemetry rolled up per 5 min'),
      (6, 'Part 3', 'ANALYTICS', 'YIELD_BY_LINE_5MIN',          'dynamic table',   'GOLD -- the two-source join'),
      (7, 'Part 3', 'ANALYTICS', 'DEFECT_COUNTS_5MIN',          'dynamic table',   'GOLD -- counts at defect grain'),
      (8, 'Part 4', 'ANALYTICS', 'PLANT_FLOOR_SV',              'semantic view',   'what the agent reads')
      AS t(seq, part, schema_name, object_name, kind, what_it_is)
),
built AS (
    SELECT table_schema AS schema_name, table_name AS object_name, row_count
    FROM MFG.INFORMATION_SCHEMA.TABLES
    UNION ALL
    SELECT "SCHEMA" AS schema_name, name AS object_name, NULL AS row_count
    FROM MFG.INFORMATION_SCHEMA.SEMANTIC_VIEWS
)
SELECT
    e.seq,
    e.part,
    e.object_name,
    e.kind,
    IFF(b.object_name IS NOT NULL, 'built', '-- NOT YET --')          AS status,
    b.row_count                                                       AS approx_rows,
    e.what_it_is
FROM expected e
LEFT JOIN built b
       ON b.schema_name = e.schema_name
      AND (b.object_name = e.object_name
           OR (e.kind = 'iceberg journal' AND STARTSWITH(b.object_name, e.object_name)))
ORDER BY e.seq;


-- =====================================================================
-- READING THE RESULT
-- =====================================================================
-- Everything through your current Part says 'built'.
--
-- QUALITY_INSPECTIONS has FEWER rows than the journal. That is the merge gate,
-- not a fault -- see Part 2.
--
-- YIELD_BY_LINE_5MIN has very few rows: three lines times however many
-- 5-minute buckets have elapsed. Nine rows after fifteen minutes is correct.
-- Do not compare it to QUALITY_INSPECTIONS and conclude something is broken.
--
-- Any table showing 'built' with 0 rows and no growth means the producer is
-- not running, or is running without the flag for that feed.
-- =====================================================================
