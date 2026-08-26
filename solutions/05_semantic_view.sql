-- =====================================================================
-- 05_semantic_view.sql   Answer key for Part 7
-- =====================================================================
-- The semantic view is what turns the Gold tables into something an agent can
-- reason about: business names, synonyms, and the metric definitions, so the
-- agent is not guessing at what a column means.
--
-- SYNTAX RULES. Cortex Code has historically generated all four of these wrong.
-- If a CREATE fails, re-emit this DDL verbatim rather than improvising:
--   1. Clause order is fixed: TABLES -> RELATIONSHIPS -> FACTS -> DIMENSIONS -> METRICS
--   2. Tables use AS, never '=':          yield AS MFG.CDC.DT_...
--   3. Synonyms use WITH SYNONYMS = (...), never a bare SYNONYMS = (...)
--   4. Metrics are alias-qualified and defined with AS:
--        yield.total_units AS SUM(yield.units)      -- correct
--        total_units = SUM(units)                   -- WRONG, will not compile
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.CDC;

CREATE OR REPLACE SEMANTIC VIEW MFG.CDC.PLANT_FLOOR_SV
  TABLES (
    yield AS MFG.CDC.DT_YIELD_BY_LINE_5MIN
      PRIMARY KEY (LINE, BUCKET)
      WITH SYNONYMS = ('yield', 'first pass yield', 'production yield', 'line output')
      COMMENT = 'Units, scrap and first-pass yield per production line per 5 minutes, with the paint booth humidity for the same interval.',
    defects AS MFG.CDC.DT_DEFECT_COUNTS_5MIN
      PRIMARY KEY (LINE, BUCKET, DEFECT_CODE)
      WITH SYNONYMS = ('defects', 'defect counts', 'scrap reasons', 'failure codes')
      COMMENT = 'Count of scans per defect code per line per 5 minutes. DEFECT_CODE = NONE means the scan passed.',
    stations AS MFG.CDC.DT_STATION_HEALTH
      PRIMARY KEY (STATION_ID, METRIC, BUCKET)
      WITH SYNONYMS = ('stations', 'station health', 'sensors', 'telemetry', 'machine metrics')
      COMMENT = 'Sensor telemetry averaged per station per metric per 5 minutes.'
  )
  RELATIONSHIPS (
    defects_to_yield AS defects (LINE, BUCKET) REFERENCES yield (LINE, BUCKET),
    stations_to_yield AS stations (LINE, BUCKET) REFERENCES yield (LINE, BUCKET)
  )
  FACTS (
    yield.units AS UNITS,
    yield.scrap_units AS SCRAP_UNITS,
    yield.yield_pct AS FIRST_PASS_YIELD_PCT,
    yield.booth_humidity AS AVG_BOOTH_HUMIDITY,
    defects.defect_n AS N,
    stations.reading_avg AS AVG_VALUE,
    stations.reading_max AS MAX_VALUE
  )
  DIMENSIONS (
    yield.line AS LINE
      WITH SYNONYMS = ('line', 'production line', 'work centre', 'stage')
      COMMENT = 'Production line: WELD, PAINT or ASSEMBLY.',
    yield.bucket AS BUCKET
      WITH SYNONYMS = ('time', 'interval', 'five minute bucket', 'when')
      COMMENT = 'Start of the 5-minute interval, UTC.',
    defects.defect_code AS DEFECT_CODE
      WITH SYNONYMS = ('defect', 'defect code', 'failure reason', 'scrap reason')
      COMMENT = 'Defect code, or NONE for a passing scan.',
    stations.station_id AS STATION_ID
      WITH SYNONYMS = ('station', 'machine', 'cell'),
    stations.metric AS METRIC
      WITH SYNONYMS = ('metric', 'sensor', 'measurement')
      COMMENT = 'One of weld_current, booth_humidity, booth_temp, torque_nm.'
  )
  METRICS (
    yield.total_units AS SUM(yield.units)
      WITH SYNONYMS = ('units produced', 'total units', 'volume'),
    yield.total_scrap AS SUM(yield.scrap_units)
      WITH SYNONYMS = ('scrap', 'total scrap', 'rejects', 'failed units'),
    yield.avg_yield_pct AS AVG(yield.yield_pct)
      WITH SYNONYMS = ('average yield', 'yield percent', 'first pass yield percent'),
    yield.avg_humidity AS AVG(yield.booth_humidity)
      WITH SYNONYMS = ('humidity', 'average booth humidity'),
    defects.defect_count AS SUM(defects.defect_n)
      WITH SYNONYMS = ('defect count', 'number of defects'),
    stations.avg_reading AS AVG(stations.reading_avg)
      WITH SYNONYMS = ('average reading', 'average sensor value'),
    stations.peak_reading AS MAX(stations.reading_max)
      WITH SYNONYMS = ('peak reading', 'max sensor value')
  )
  COMMENT = 'Cascade Cycleworks plant floor: yield, scrap, defects and station telemetry at a 5-minute grain.';


-- =====================================================================
-- CHECKPOINTS -- the three questions the agent has to answer in Part 6
-- =====================================================================
-- Query a semantic view with SEMANTIC_VIEW(), naming DIMENSIONS and METRICS.
-- If these three work, the agent has what it needs.

-- Q1  "What is first-pass yield by line right now?"
SELECT * FROM SEMANTIC_VIEW(MFG.CDC.PLANT_FLOOR_SV
  DIMENSIONS yield.line
  METRICS yield.avg_yield_pct, yield.total_units, yield.total_scrap)
ORDER BY 1;

-- Q2  "Which defect is driving scrap on PAINT?"
SELECT * FROM SEMANTIC_VIEW(MFG.CDC.PLANT_FLOOR_SV
  DIMENSIONS defects.defect_code
  METRICS defects.defect_count
  WHERE yield.line = 'PAINT' AND defects.defect_code <> 'NONE')
ORDER BY 2 DESC;

-- Q3  "Why did PAINT yield drop?"  <- the payoff, and the reason the second
--     data source exists. Yield and booth humidity in the same result set,
--     bucket by bucket. During the incident you see humidity climb from ~44
--     into the 60s-70s while yield falls from ~99% to the mid 70s.
SELECT * FROM SEMANTIC_VIEW(MFG.CDC.PLANT_FLOOR_SV
  DIMENSIONS yield.line, yield.bucket
  METRICS yield.avg_yield_pct, yield.avg_humidity
  WHERE yield.line = 'PAINT')
ORDER BY 2 DESC
LIMIT 6;
