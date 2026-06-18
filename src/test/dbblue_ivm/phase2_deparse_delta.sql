-- DBblue IVM — Phase 2 deparse delta core (plain single-table aggregate).
--
-- The delta SQL for a plain single-table aggregate matview can be generated two
-- ways: the hand-written string builders (default) or the query-tree deparse
-- core (dbblue_ivm_deparse_delta = on), which copies the stored view query,
-- swaps the source relation for its transition-table ENR, and lets ruleutils
-- render the SELECT.  This test proves:
--
--   1. The two paths are EQUIVALENT — every aggregate shape both can express is
--      maintained identically (verified against a live recompute, 0 diffs) with
--      the GUC off and on.
--   2. Aggregate arguments that are genuine single-argument function calls
--      (e.g. SUM(floor(amt))) are maintained CORRECTLY on both paths.  The hand
--      deparser used to treat every single-argument FuncExpr as a cast and
--      silently drop the function; that is fixed, and the deparse core renders
--      such expressions natively.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: Phase 2 deparse delta core ==='
\echo ''

DROP TABLE IF EXISTS p2d CASCADE;
CREATE TABLE p2d(id serial PRIMARY KEY, g int, amt numeric, qty int);
INSERT INTO p2d(g,amt,qty) SELECT i%4, (i + 0.7)::numeric, i FROM generate_series(1,40) i;

-- live-recompute comparator: 0 means the matview equals a from-scratch SELECT
CREATE OR REPLACE FUNCTION _p2cmp(mv text, cols text, live text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
  EXECUTE format('SELECT (SELECT count(*) FROM (SELECT %2$s FROM %1$s EXCEPT %3$s) a)
                       + (SELECT count(*) FROM (%3$s EXCEPT SELECT %2$s FROM %1$s) b)',
                 mv, cols, live) INTO n;
  RETURN n;
END $$;

-- Exercise a matview through a full INSERT/UPDATE/DELETE lifecycle and assert
-- it matches a live recompute.  Run once per GUC state.
CREATE OR REPLACE FUNCTION _p2run(deparse bool) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  live text := 'SELECT g AS k, SUM(amt) s, COUNT(*) c, AVG(amt) a,'
               ' SUM(amt*qty) sx, SUM(floor(amt)) sf FROM p2d GROUP BY g';
  n int;
BEGIN
  EXECUTE 'SET dbblue_ivm_deparse_delta = ' || deparse::text;
  EXECUTE 'CREATE MATERIALIZED VIEW p2d_mv WITH (incremental_refresh=true) AS ' || live;

  INSERT INTO p2d(g,amt,qty) SELECT i%4, (i + 0.3)::numeric, i FROM generate_series(41,80) i;
  DELETE FROM p2d WHERE id % 7 = 0;
  UPDATE p2d SET amt = amt + 5, qty = qty + 1 WHERE id % 5 = 0;
  INSERT INTO p2d(g,amt,qty) VALUES (2, 1000.25, 3);

  SELECT _p2cmp('p2d_mv', 'k,s,c,a,sx,sf', live) INTO n;
  IF n = 0 THEN
    RAISE NOTICE 'deparse=%: plain-agg lifecycle correct (incl. SUM(amt*qty), SUM(floor(amt))): PASS', deparse;
  ELSE
    RAISE EXCEPTION 'deparse=%: FAIL (% diff vs live recompute)', deparse, n;
  END IF;

  EXECUTE 'DROP MATERIALIZED VIEW p2d_mv';
END $$;

-- 1+2. Equivalence + single-arg-function correctness, hand path then deparse path.
SELECT _p2run(false);   -- hand builders
SELECT _p2run(true);    -- deparse core

-- The stored delta SQL on the deparse path must render the function natively
-- (not as a dropped cast) and name the transition table.
SET dbblue_ivm_deparse_delta = on;
CREATE MATERIALIZED VIEW p2d_mv WITH (incremental_refresh=true) AS
  SELECT g AS k, SUM(floor(amt)) sf, COUNT(*) c FROM p2d GROUP BY g;
DO $$
DECLARE s text;
BEGIN
  SELECT ins_sql INTO s FROM pg_dbblue_matview WHERE mvrelid='p2d_mv'::regclass;
  IF s LIKE '%floor(amt)%' AND s LIKE '%__mv_newtable%' THEN
    RAISE NOTICE 'deparse renders floor(amt) natively over the ENR: PASS';
  ELSE
    RAISE EXCEPTION 'deparse delta SQL missing floor(amt)/ENR: FAIL (%)', s;
  END IF;
END $$;
DROP MATERIALIZED VIEW p2d_mv;

DROP FUNCTION _p2run(bool);
DROP FUNCTION _p2cmp(text,text,text);
DROP TABLE p2d CASCADE;
RESET dbblue_ivm_deparse_delta;
\echo ''
\echo '=== Phase 2 deparse delta core test complete ==='
