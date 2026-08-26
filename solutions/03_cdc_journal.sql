-- =====================================================================
-- 03_cdc_journal.sql   Answer key for Part 1 (build) and Part 2 (inspect)
-- =====================================================================
-- This is the part of the lab that is actually about change data capture.
--
-- The Openflow PostgreSQL CDC connector does NOT write your destination table
-- directly. It writes a JOURNAL -- an append-only log of change events -- and a
-- merge processor applies that journal to the destination.
--
-- IMPORTANT, because it is easy to assume otherwise: the connector does **not**
-- create a Snowflake TASK. Its merge processor runs inside the connector runtime
-- and issues the MERGE itself, over its own Snowflake connection. A CRON
-- expression acts as an internal *eligibility gate* deciding when queued changes
-- become mergeable -- the flow default is `0 * * * * ?`, second :00 of every
-- minute. So in this lab the producer issues the MERGE too, on the same gate.
-- There is no task to create here, and you should not add one.
--
-- Three observable behaviours come out of that design, and all three are worth
-- seeing:
--
--   1. Soft deletes. A voided row is flagged, never removed.
--   2. A merge gate. The destination lags the journal by up to a minute, and
--      that lag is a *scheduling* decision, not a throughput limit.
--   3. Two paths. The initial snapshot loads the destination directly; ongoing
--      changes go through the journal. Same table, two very different writers.
--
-- You INSPECT the journal in this lab. You never build Dynamic Tables on it --
-- it is connector-internal, its schema shifts with the generation counter, and
-- the connector prunes it. Build on the destination table.
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.RAW;          -- required before any Iceberg CREATE; see 01_environment.sql

-- ---------------------------------------------------------------------
-- The journal table.
--
-- Naming is the connector's: "<TABLE>_JOURNAL_<series>_<generation>", where
-- series is epoch seconds at table registration and generation starts at 1 and
-- increments on every schema-changing DDL. Only the highest generation is
-- active. We PIN the series here so the lab has stable object names; in
-- production you cannot predict it, which is part of why you never build
-- anything durable on the journal.
--
-- Column order is the connector's and matters:
--   PRIMARY_KEY__<col>  one per replication key column, NOT NULL
--   PAYLOAD__<col>      one per EVERY source column, including the key
--   LEAST_/MOST_SIGNIFICANT_POSITION   the WAL position, used for ordering
--   EVENT_TYPE, SEEN_AT, SF_METADATA
--
-- The key appears TWICE, as PRIMARY_KEY__INSPECTION_ID and PAYLOAD__INSPECTION_ID. That is
-- deliberate: it is what makes a primary-key change detectable
-- (PRIMARY_KEY__k <> PAYLOAD__k). This lab's key never changes, so the MERGE
-- below omits the key-change machinery -- see the note at the end.
--
-- SF_METADATA is VARIANT, which is exactly why this table needs Iceberg v3.
-- ICEBERG_VERSION = 3 is stated explicitly rather than inherited, because a
-- silent fall back to v2 here fails with "Unsupported data type 'VARIANT'".
-- ---------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1 (
  PRIMARY_KEY__INSPECTION_ID        STRING        NOT NULL,
  PAYLOAD__INSPECTION_ID            STRING,
  PAYLOAD__UNIT_ID           STRING,
  PAYLOAD__LINE               STRING,
  PAYLOAD__SKU                STRING,
  PAYLOAD__STATUS             STRING,
  PAYLOAD__DEFECT_CODE        STRING,
  PAYLOAD__STATION_ID         STRING,
  PAYLOAD__OPERATOR_ID        STRING,
  PAYLOAD__EVENT_TS           TIMESTAMP_NTZ,
  PAYLOAD__UPDATED_TS         TIMESTAMP_NTZ,
  LEAST_SIGNIFICANT_POSITION  NUMBER(38,0),   -- bare NUMBER is rejected by Iceberg
  MOST_SIGNIFICANT_POSITION   NUMBER(38,0),
  EVENT_TYPE                  STRING        NOT NULL,
  SEEN_AT                     TIMESTAMP_NTZ,
  SF_METADATA                 VARIANT
)
  ICEBERG_VERSION = 3
  ERROR_LOGGING = TRUE;      -- the connector sets this; bad rows land in an error table

-- ---------------------------------------------------------------------
-- The stream. APPEND_ONLY, because a journal only ever gets appends.
--
-- Reading the STREAM rather than the table is what gives exactly-once: the
-- offset advances only when the consuming DML commits. Note that append-only
-- streams on *externally managed* Iceberg tables are unsupported -- on
-- Snowflake-managed v3, as here, they work.
-- ---------------------------------------------------------------------
CREATE OR REPLACE STREAM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM
  ON TABLE MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1
  APPEND_ONLY = TRUE;

-- That is all the DDL. Two objects. No task, no pipe.


-- =====================================================================
-- THE MERGE -- for reference, and to run by hand
-- =====================================================================
-- The producer issues this statement on its CRON gate, exactly as the connector
-- does, with the connector's QUERY_TAG set so you can find it in QUERY_HISTORY.
-- You do not need to run it yourself; it is here so you can read it, and so you
-- can apply a batch on demand if you want to see the effect immediately.
--
-- Invariants it preserves, all of which matter:
--   1. Read the STREAM, not the journal table. Consuming the stream inside a
--      committed statement is what advances the offset.
--   2. Dedup to ONE row per replication key, ordered by the LSN tuple DESC.
--      Without this, an insert-then-update in the same batch applies in
--      arbitrary order.
--   3. Soft delete only -- never DELETE FROM the destination.
--   4. The ON clause joins SOURCE.PRIMARY_KEY__<k> to TARGET.<k>, prefix stripped.
--   5. The INSERT branch falls back to PRIMARY_KEY__ for delete tombstones,
--      because a DELETE event carries no payload at all.
--
-- ALTER SESSION SET QUERY_TAG = '{"application":"SNOWFLAKE_OPENFLOW","operation":"cdc.merge.full_values","strategy":"full_values_snowflake_managed"}';
--
-- MERGE INTO MFG.RAW.QUALITY_INSPECTIONS AS TARGET
-- USING (
--     SELECT * FROM (
--         SELECT PRIMARY_KEY__INSPECTION_ID,
--                PAYLOAD__INSPECTION_ID, PAYLOAD__UNIT_ID, PAYLOAD__LINE, PAYLOAD__SKU,
--                PAYLOAD__STATUS, PAYLOAD__DEFECT_CODE, PAYLOAD__STATION_ID,
--                PAYLOAD__OPERATOR_ID, PAYLOAD__EVENT_TS, PAYLOAD__UPDATED_TS,
--                EVENT_TYPE,
--                ROW_NUMBER() OVER (
--                    PARTITION BY PRIMARY_KEY__INSPECTION_ID
--                    ORDER BY MOST_SIGNIFICANT_POSITION DESC,
--                             LEAST_SIGNIFICANT_POSITION DESC
--                ) AS ROW_NUM
--         FROM MFG.RAW.QUALITY_INSPECTIONS_JOURNAL_1787700000_1_STREAM
--         WHERE EVENT_TYPE IN ('IncrementalInsertRows',
--                              'IncrementalUpdateRows',
--                              'IncrementalDeleteRows')
--     ) WHERE ROW_NUM = 1
-- ) AS SOURCE
-- ON SOURCE.PRIMARY_KEY__INSPECTION_ID = TARGET.INSPECTION_ID
-- WHEN MATCHED AND SOURCE.EVENT_TYPE IN ('IncrementalInsertRows','IncrementalUpdateRows') THEN
--     UPDATE SET TARGET.INSPECTION_ID               = SOURCE.PAYLOAD__INSPECTION_ID,
--                TARGET.UNIT_ID              = SOURCE.PAYLOAD__UNIT_ID,
--                TARGET.LINE                  = SOURCE.PAYLOAD__LINE,
--                TARGET.SKU                   = SOURCE.PAYLOAD__SKU,
--                TARGET.STATUS                = SOURCE.PAYLOAD__STATUS,
--                TARGET.DEFECT_CODE           = SOURCE.PAYLOAD__DEFECT_CODE,
--                TARGET.STATION_ID            = SOURCE.PAYLOAD__STATION_ID,
--                TARGET.OPERATOR_ID           = SOURCE.PAYLOAD__OPERATOR_ID,
--                TARGET.EVENT_TS              = SOURCE.PAYLOAD__EVENT_TS,
--                TARGET.UPDATED_TS            = SOURCE.PAYLOAD__UPDATED_TS,
--                TARGET._SNOWFLAKE_DELETED    = FALSE,
--                TARGET._SNOWFLAKE_UPDATED_AT = SYSDATE()
-- WHEN MATCHED AND SOURCE.EVENT_TYPE = 'IncrementalDeleteRows' THEN
--     UPDATE SET TARGET._SNOWFLAKE_DELETED    = TRUE,
--                TARGET._SNOWFLAKE_UPDATED_AT = SYSDATE()
-- WHEN NOT MATCHED THEN
--     INSERT (INSPECTION_ID, UNIT_ID, LINE, SKU, STATUS, DEFECT_CODE, STATION_ID,
--             OPERATOR_ID, EVENT_TS, UPDATED_TS,
--             _SNOWFLAKE_INSERTED_AT, _SNOWFLAKE_UPDATED_AT, _SNOWFLAKE_DELETED)
--     VALUES (IFF(SOURCE.EVENT_TYPE = 'IncrementalDeleteRows',
--                 SOURCE.PRIMARY_KEY__INSPECTION_ID, SOURCE.PAYLOAD__INSPECTION_ID),
--             SOURCE.PAYLOAD__UNIT_ID, SOURCE.PAYLOAD__LINE, SOURCE.PAYLOAD__SKU,
--             SOURCE.PAYLOAD__STATUS, SOURCE.PAYLOAD__DEFECT_CODE,
--             SOURCE.PAYLOAD__STATION_ID, SOURCE.PAYLOAD__OPERATOR_ID,
--             SOURCE.PAYLOAD__EVENT_TS, SOURCE.PAYLOAD__UPDATED_TS,
--             SYSDATE(), SYSDATE(),
--             IFF(SOURCE.EVENT_TYPE = 'IncrementalDeleteRows', TRUE, FALSE));


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

-- 8. Tighten the gate and watch the lag fall. This is the honest lesson about
--    where the latency actually lives: it is a schedule, and you choose it.
--    Restart the producer with a shorter gate and re-run checkpoint 4:
--
--      producer.py --profile producer/profile.json --cdc --telemetry \
--                  --merge-gate-seconds 10
--
--    In a real deployment this is the connector's `Merge Task Schedule CRON`
--    parameter. The trade is warehouse time: more merges, smaller batches.


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
