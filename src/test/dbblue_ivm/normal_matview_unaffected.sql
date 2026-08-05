-- DBblue IVM — normal (non-incremental) matviews must be UNAFFECTED.
--
-- The incremental engine is strictly opt-in via WITH (incremental_refresh=true).
-- A plain materialized view must behave exactly like stock PostgreSQL: every
-- shape the IVM engine rejects must still work, REFRESH must be correct
-- (including NULL groups), and the source-table DDL guards / dependency hints
-- must NOT apply to it.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: normal matviews unaffected ==='
\echo ''

DROP TABLE IF EXISTS nmu_t, nmu_dim CASCADE;
CREATE TABLE nmu_t(id serial PRIMARY KEY, g int, amt double precision, state text, d date);
CREATE TABLE nmu_dim(id int PRIMARY KEY, nm text);
INSERT INTO nmu_t(g,amt,state,d) SELECT i%5, i*1.1, 'x', '2024-01-01'::date+i FROM generate_series(1,20) i;
INSERT INTO nmu_dim SELECT i,'n'||i FROM generate_series(0,4) i;

-- Every IVM-rejected shape must succeed as a normal matview.
CREATE OR REPLACE FUNCTION _mk(sql text) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _n AS '||sql;
  EXECUTE 'DROP MATERIALIZED VIEW _n';
  RETURN true;
EXCEPTION WHEN OTHERS THEN
  RETURN false;
END $$;

DO $$
DECLARE ok bool := true;
BEGIN
  ok := ok AND _mk('SELECT g, COUNT(DISTINCT state) FROM nmu_t GROUP BY g');
  ok := ok AND _mk('SELECT g, COUNT(*) FILTER (WHERE state=''x'') FROM nmu_t GROUP BY g');
  ok := ok AND _mk('SELECT date_trunc(''month'',d) m, SUM(amt) FROM nmu_t GROUP BY date_trunc(''month'',d)');
  ok := ok AND _mk('SELECT g, SUM(amt) FROM nmu_t GROUP BY g');                       -- float
  ok := ok AND _mk('SELECT g, amt, ROW_NUMBER() OVER (PARTITION BY g ORDER BY amt) FROM nmu_t'); -- window
  ok := ok AND _mk('WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT * FROM r');
  ok := ok AND _mk('SELECT g, SUM(amt) FROM nmu_t WHERE g IN (SELECT id FROM nmu_dim) GROUP BY g');
  ok := ok AND _mk('SELECT SUM(s) FROM (SELECT g, SUM(amt) s FROM nmu_t GROUP BY g) q'); -- nested agg
  IF ok THEN RAISE NOTICE 'all IVM-rejected shapes work as normal matviews: PASS';
  ELSE RAISE EXCEPTION 'a normal matview shape was rejected: FAIL'; END IF;
END $$;

DROP FUNCTION _mk(text);

-- Normal matview REFRESH must be correct, including the NULL group.
DROP MATERIALIZED VIEW IF EXISTS nmu_mv CASCADE;
INSERT INTO nmu_t(g,amt) VALUES (NULL, 5);
CREATE MATERIALIZED VIEW nmu_mv AS SELECT g, SUM(amt) s FROM nmu_t GROUP BY g;
INSERT INTO nmu_t(g,amt) VALUES (1, 100), (NULL, 7);
REFRESH MATERIALIZED VIEW nmu_mv;
DO $$
DECLARE diff int;
BEGIN
  SELECT count(*) INTO diff FROM (
    (SELECT g,s FROM nmu_mv EXCEPT SELECT g, SUM(amt) FROM nmu_t GROUP BY g)
    UNION ALL
    (SELECT g, SUM(amt) FROM nmu_t GROUP BY g EXCEPT SELECT g,s FROM nmu_mv)
  ) x;
  IF diff = 0 THEN RAISE NOTICE 'normal REFRESH correct incl. NULL group: PASS';
  ELSE RAISE EXCEPTION 'normal REFRESH: FAIL (% diff)', diff; END IF;
END $$;

-- Renaming a column a normal matview uses must NOT be blocked (IVM guard is opt-in).
DO $$
BEGIN
  ALTER TABLE nmu_t RENAME COLUMN amt TO amt2;
  ALTER TABLE nmu_t RENAME COLUMN amt2 TO amt;   -- rename back
  RAISE NOTICE 'rename of normal-matview column allowed: PASS';
EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION 'rename of normal-matview column was blocked: FAIL';
END $$;

-- DROP errors must use standard PG wording (no incremental-matview hint).
DO $$
DECLARE h text;
BEGIN
  BEGIN
    EXECUTE 'DROP TABLE nmu_t';
    RAISE EXCEPTION 'DROP TABLE with dependent normal matview: FAIL (allowed)';
  EXCEPTION WHEN dependent_objects_still_exist THEN
    GET STACKED DIAGNOSTICS h = PG_EXCEPTION_HINT;
    IF h ILIKE '%incremental materialized view%'
    THEN RAISE EXCEPTION 'normal-matview DROP hint leaked incremental wording: FAIL';
    ELSE RAISE NOTICE 'normal-matview DROP uses standard wording: PASS'; END IF;
  END;
END $$;

DROP MATERIALIZED VIEW nmu_mv CASCADE;
DROP TABLE nmu_t, nmu_dim CASCADE;
\echo ''
\echo '=== normal matviews unaffected test complete ==='
