-- =====================================================================
-- 03_journal_inspection.sql   Answer key for Part 2
-- =====================================================================
-- You do NOT create the journal, its stream, or the destination table. The
-- connector creates all three for itself when you start it in Part 2, exactly as
-- a real Openflow connector does -- see producer/cdc_simulator.py, ensure_objects().
--
-- This file is what you use to LOOK at what it built and what it is doing:
-- the change events, the three event shapes, the SF_METADATA quirk, and above all
-- the merge gate.
--
-- The DDL used to live here. It moved into the connector because having attendees
-- hand-build a CDC destination table taught the wrong division of labour: in
-- production nobody does that, you point the connector at a source and the objects
-- appear.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.RAW;

-- =====================================================================
-- THE MERGE -- what it does, and where to read it
-- =====================================================================
-- The producer issues this statement on its CRON gate, exactly as the connector
-- does, with the connector's QUERY_TAG set so you can find it in QUERY_HISTORY.
--
-- The statement itself lives in producer/cdc_simulator.py, as MERGE_SQL. Read it
-- there rather than here: that is the copy that actually runs, so it cannot be
-- out of date.
--
-- Invariants it preserves, all of which matter:
--   1. Read the STREAM, not the journal table. Consuming the stream inside a
--      committed statement is what advances the offset. That is what makes it
--      exactly-once.
--   2. Dedup to ONE row per replication key, ordered by the LSN tuple DESC.
--      Without this, an insert-then-update in the same batch applies in
--      arbitrary order.
--   3. Soft delete only -- never DELETE FROM the destination.
--   4. The ON clause joins SOURCE.PRIMARY_KEY__<k> to TARGET.<k>, prefix stripped.
--   5. The INSERT branch falls back to PRIMARY_KEY__ for delete tombstones,
--      because a DELETE event carries no payload at all.
--
-- You do not need to run it yourself -- the producer does, once a minute.


-- =====================================================================
-- CHECKPOINTS -- run these while the producer is going
-- =====================================================================

-- 1. Raw change events, newest first. This is the CDC wire format.
--    Look at what each EVENT_TYPE carries:
--      IncrementalInsertRows  every PAYLOAD__* populated
--      IncrementalUpdateRows  PAYLOAD__* holds the NEW values
--      IncrementalDeleteRows  every PAYLOAD__* is NULL -- key only
SELECT EVENT_TYPE, PRIMARY_KEY__INSPECTION_ID, PAYLOAD__STATUS, PAYLOAD__DEFECT_CODE,
       MOST_SIGNIFICANT_POSITION AS txn_lsn, LEAST_SIGNIFICANT_POSITION AS msg_lsn, SEEN_AT
FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
ORDER BY MOST_SIGNIFICANT_POSITION DESC, LEAST_SIGNIFICANT_POSITION DESC
LIMIT 20;

-- 2. Event mix.
SELECT EVENT_TYPE, COUNT(*) AS n
FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
GROUP BY 1 ORDER BY 2 DESC;

-- 3. SF_METADATA is a VARIANT that holds a JSON *string*, not a parsed object --
--    the connector writes it that way. So this returns NULL:
--        SELECT SF_METADATA:offset_token FROM ...
--    and this works:
SELECT SF_METADATA                                          AS raw_variant,
       TYPEOF(SF_METADATA)                                  AS what_it_really_is,
       PARSE_JSON(SF_METADATA::STRING):offset_token::STRING  AS offset_token
FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
LIMIT 3;

-- 4. THE MERGE GATE, which is the point of this section.
--    The journal always leads the destination. The gap is the scheduling gate,
--    not a throughput limit -- the merge itself takes a second or two.
SELECT (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
         WHERE EVENT_TYPE = 'IncrementalInsertRows')          AS journal_inserts,
       (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS)        AS destination_rows,
       (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
         WHERE EVENT_TYPE = 'IncrementalInsertRows')
         - (SELECT COUNT(*) FROM MFG.RAW.QUALITY_INSPECTIONS)    AS awaiting_merge,
       SYSTEM$STREAM_HAS_DATA(
         'MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM')
                                                              AS stream_has_pending;

-- 5. The merges themselves, found the way you would find them in production:
--    by the connector's QUERY_TAG. This is the payoff of the tag, and it is
--    exactly how you would audit a real Openflow deployment.
--
--    Note the START_TIME of each one: second :00, every minute. That is the CRON
--    eligibility gate. Note also TOTAL_ELAPSED_TIME -- a second or two. The
--    latency is the gate, not the merge.
SELECT TO_VARCHAR(START_TIME, 'HH24:MI:SS')        AS fired_at,
       QUERY_TYPE,
       ROWS_PRODUCED                              AS rows_affected,
       ROUND(TOTAL_ELAPSED_TIME / 1000, 1)        AS merge_seconds,
       WAREHOUSE_NAME,
       PARSE_JSON(QUERY_TAG):operation::STRING    AS connector_operation,
       PARSE_JSON(QUERY_TAG):strategy::STRING     AS merge_strategy
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 500))
WHERE QUERY_TAG LIKE '%SNOWFLAKE_OPENFLOW%'
ORDER BY START_TIME DESC
LIMIT 10;

-- 6. Soft delete, end to end. A voided scan is still physically in the
--    destination, flagged. Everything downstream must filter it -- which is
--    exactly what INSPECTIONS_ACTIVE does in the next file.
SELECT INSPECTION_ID, LINE, STATUS, DEFECT_CODE, _SNOWFLAKE_DELETED,
       _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT
FROM MFG.RAW.QUALITY_INSPECTIONS
WHERE _SNOWFLAKE_DELETED
LIMIT 5;

-- 7. An overturned frame: _SNOWFLAKE_UPDATED_AT has moved past
--    _SNOWFLAKE_INSERTED_AT, and STATUS is now PASS with no defect code. The
--    MERGE rewrote a row that had already been reported on.
SELECT INSPECTION_ID, LINE, STATUS, DEFECT_CODE,
       _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT
FROM MFG.RAW.QUALITY_INSPECTIONS
WHERE _SNOWFLAKE_UPDATED_AT > _SNOWFLAKE_INSERTED_AT AND NOT _SNOWFLAKE_DELETED
LIMIT 5;

-- 8. Where the latency actually lives: it is a schedule, and you choose it. The gate
--    defaults to 60s -- the connector's own flow default of second :00 every minute.
--    In a real deployment you change it with the connector's `Merge Task Schedule CRON`
--    parameter; here it is `--merge-gate-seconds`. The trade is warehouse time: more
--    merges, smaller batches.
--
--    Changing it means restarting the producer, which this lab never asks you to do --
--    so this is a knob to understand, not one to turn today. If you rehearse the lab
--    from a shell later, start it with `--merge-gate-seconds 10` and re-run checkpoint
--    4 to watch the gap shrink.


-- =====================================================================
-- WHAT THIS SIMULATION LEAVES OUT -- stated, not hidden
-- =====================================================================
-- 1. PRIMARY KEY CHANGES. A real connector handles an UPDATE that changes the
--    replication key by turning it into a soft-delete of the old key plus an
--    insert of the new one, detected with PRIMARY_KEY__k <> PAYLOAD__k and
--    materialised with a CROSS JOIN that splits the row in two. INSPECTION_ID never
--    changes in this lab, so that machinery is omitted for readability.
-- 2. SCHEMA EVOLUTION. A schema-changing DDL writes a SchemaChangedRows marker,
--    bumps the generation, and creates a NEW journal table and stream. We pin
--    generation 1.
-- 3. THE SNAPSHOT PATH. A real connector first bulk-loads the destination table,
--    then switches to the journal for ongoing changes. Here the producer starts
--    from an empty table, so every row arrives as CDC.
-- 4. CONCURRENCY. The real merge processor runs up to 4 concurrent tasks with
--    per-table mutual exclusion and async query submission with retries. The
--    producer runs one merge at a time, serially.
-- 5. JOURNAL PRUNING, re-snapshot archive tables, and the TOAST / unchanged-value
--    placeholder handling for wide source rows.
-- 6. Real WAL LSNs. The producer uses a monotonic logical clock instead, which is
--    all the ordering guarantee the MERGE actually requires.

-- A trailing real statement, deliberately: Snowsight parses text after the last
-- statement as a statement, so a file ENDING in comments throws
-- "SQL compilation error: Empty SQL statement" when you run the whole thing.
SELECT 'journal and stream created -- no task, by design' AS status;
