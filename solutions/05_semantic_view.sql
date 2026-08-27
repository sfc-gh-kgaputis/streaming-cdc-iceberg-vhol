-- =====================================================================
-- 05_semantic_view.sql   Answer key for Part 4
-- =====================================================================
-- The semantic view is what turns the Gold tables into something an agent can
-- reason about: business names, synonyms, and the metric definitions, so the
-- agent is not guessing at what a column means.
--
-- SYNTAX RULES. Emit all four exactly as written; each is a form that is easy to
-- get wrong. If a CREATE fails, re-emit this DDL verbatim rather than improvising:
--   1. Clause order is fixed: TABLES -> RELATIONSHIPS -> FACTS -> DIMENSIONS -> METRICS
--   2. Tables use AS, never '=':          yield AS MFG.ANALYTICS....
--   3. Synonyms use WITH SYNONYMS = (...), never a bare SYNONYMS = (...)
--   4. Metrics are alias-qualified and defined with AS:
--        yield.total_units AS SUM(yield.units)      -- correct
--        total_units = SUM(units)                   -- WRONG, will not compile
-- =====================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOL_WH;
USE SCHEMA MFG.ANALYTICS;

-- THREE tables, not two. The agent needs stations even though
-- YIELD_BY_LINE_5MIN already carries booth humidity, because "is anything
-- wrong on WELD?"
-- is a question about a metric that never reaches the yield table.
CREATE OR REPLACE SEMANTIC VIEW MFG.ANALYTICS.PLANT_FLOOR_SV
  TABLES (
    yield AS MFG.ANALYTICS.YIELD_BY_LINE_5MIN
      PRIMARY KEY (LINE, BUCKET)
      WITH SYNONYMS = ('yield', 'first pass yield', 'production yield', 'line output')
      COMMENT = 'Units, scrap and first-pass yield per production line per 5 minutes, with the paint booth humidity for the same interval.',
    defects AS MFG.ANALYTICS.DEFECT_COUNTS_5MIN
      PRIMARY KEY (LINE, BUCKET, DEFECT_CODE)
      WITH SYNONYMS = ('defects', 'defect counts', 'scrap reasons', 'failure codes')
      COMMENT = 'Count of scans per defect code per line per 5 minutes. DEFECT_CODE = NONE means the scan passed.',
    stations AS MFG.ANALYTICS.STATION_HEALTH
      PRIMARY KEY (STATION_ID, METRIC, BUCKET)
      WITH SYNONYMS = ('stations', 'station health', 'sensors', 'telemetry', 'machine metrics')
      COMMENT = 'Sensor telemetry averaged per station per metric per 5 minutes.'
  )
  -- Both relationships point AT yield, making it the hub. That is deliberate:
  -- every question in this lab is ultimately "how is the line doing", so the
  -- agent should reach defects and telemetry by way of yield rather than
  -- joining them to each other. A star beats a chain for text-to-SQL, because
  -- there is only ever one join path to get wrong.
  RELATIONSHIPS (
    defects_to_yield AS defects (LINE, BUCKET) REFERENCES yield (LINE, BUCKET),
    stations_to_yield AS stations (LINE, BUCKET) REFERENCES yield (LINE, BUCKET)
  )
  -- FACTS are raw columns; METRICS are aggregations over them. The split
  -- matters: an agent handed only raw columns invents its own aggregations and
  -- picks a different one each time you ask. Naming the metric once here is
  -- what makes two runs of the same question return the same number.
  FACTS (
    yield.units AS UNITS,
    yield.scrap_units AS SCRAP_UNITS,
    yield.yield_pct AS FIRST_PASS_YIELD_PCT,
    yield.booth_humidity AS AVG_BOOTH_HUMIDITY,
    defects.defect_n AS N,
    stations.reading_avg AS AVG_VALUE,
    stations.reading_max AS MAX_VALUE
  )
  -- Add a synonym for every word a plant manager uses for a column. A plant
  -- manager says "work centre" and "stage"; the column is called LINE. Every
  -- synonym you add is a question that now resolves without clarification.
  -- COMMENTs do the same job for values -- note the two places that spell out
  -- what NONE means, because "the top defect is NONE" is the classic wrong answer.
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
    -- Weighted, not AVG(yield_pct). Averaging per-bucket percentages gives a
    -- partial 20-unit bucket the same weight as a full 200-unit one, so the
    -- metric contradicts TOTAL_UNITS and TOTAL_SCRAP in its own result row.
    -- At a single bucket's grain the two are identical; they diverge only when
    -- the agent aggregates across buckets, which is exactly what "right now"
    -- and "the last thirty minutes" both do.
    yield.avg_yield_pct AS ROUND(100 * (SUM(yield.units) - SUM(yield.scrap_units)) / NULLIF(SUM(yield.units), 0), 2)
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
-- CHECKPOINTS -- the three questions the agent has to answer in Parts 4 and 5
-- =====================================================================
-- Query a semantic view with SEMANTIC_VIEW(), naming DIMENSIONS and METRICS.
-- If these three work, the agent has what it needs. And if the agent later gets
-- one of them wrong, you know the problem is in its instructions, not in this
-- view. That is why you run them by hand first.

-- Q1  "What is first-pass yield by line right now?"
SELECT * FROM SEMANTIC_VIEW(MFG.ANALYTICS.PLANT_FLOOR_SV
  DIMENSIONS yield.line
  METRICS yield.avg_yield_pct, yield.total_units, yield.total_scrap)
ORDER BY 1;

-- Q2  "Which defect is driving scrap on PAINT?"
SELECT * FROM SEMANTIC_VIEW(MFG.ANALYTICS.PLANT_FLOOR_SV
  DIMENSIONS defects.defect_code
  METRICS defects.defect_count
  WHERE yield.line = 'PAINT' AND defects.defect_code <> 'NONE')
ORDER BY 2 DESC;

-- Q3  "Why did PAINT yield drop?"  <- needs both data sources at once.
--     Yield and booth humidity in the same result set,
--     bucket by bucket. During the incident you see humidity climb from ~44
--     into the 60s-70s while yield falls from ~97% to the mid-to-high 70s.
SELECT * FROM SEMANTIC_VIEW(MFG.ANALYTICS.PLANT_FLOOR_SV
  DIMENSIONS yield.line, yield.bucket
  METRICS yield.avg_yield_pct, yield.avg_humidity
  WHERE yield.line = 'PAINT')
ORDER BY 2 DESC
LIMIT 6;
