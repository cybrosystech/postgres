-- DBblue IVM — expression aggregate arguments (auto-routed deparse core).
--
-- A plain single-table aggregate may use any DETERMINISTIC (immutable) scalar
-- expression as an aggregate argument — CASE, COALESCE, function calls — that
-- the hand-written delta grammar cannot express.  Such a shape is AUTO-ROUTED
-- to the query-tree deparse core regardless of the dbblue_ivm_deparse_delta
-- GUC, so it is maintainable AND restorable under default settings.
--
-- Safety rails (must still reject):
--   * volatile / stable arguments (would drift: the same row's insert-delta and
--     later delete-delta wouldn't cancel),
--   * nested aggregate / window / subquery arguments,
--   * expression arguments in shapes deparse is NOT wired for (mixed MIN/MAX,
--     HAVING, JOIN) — those keep the restricted hand grammar.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: expression aggregate arguments ==='
\echo ''

DROP TABLE IF EXISTS ea CASCADE;
CREATE TABLE ea(id serial PRIMARY KEY, g int, amt numeric, st text, ts timestamptz DEFAULT now());
INSERT INTO ea(g,amt,st) SELECT i%3, (i+0.5)::numeric, (ARRAY['new','done','cancel'])[1+i%3]
  FROM generate_series(1,30) i;

-- 1. SUM(CASE) + AVG(COALESCE) maintained correctly through a full lifecycle,
--    under the DEFAULT GUC (auto-routed).
SHOW dbblue_ivm_deparse_delta;   -- expected: off
CREATE MATERIALIZED VIEW ea_mv WITH (incremental_refresh=true) AS
  SELECT g AS k,
         SUM(CASE WHEN st='done' THEN amt ELSE 0 END) AS done_amt,
         AVG(COALESCE(amt, 0)) AS avg_amt,
         COUNT(*) AS c
  FROM ea GROUP BY g;

DO $$
DECLARE s text;
BEGIN
  SELECT ins_sql INTO s FROM pg_dbblue_matview WHERE mvrelid='ea_mv'::regclass;
  IF s LIKE '%CASE%' AND s LIKE '%__mv_newtable%'
  THEN RAISE NOTICE 'SUM(CASE) auto-routed to deparse core under default GUC: PASS';
  ELSE RAISE EXCEPTION 'SUM(CASE) not auto-routed to deparse: FAIL (%)', s; END IF;
END $$;

INSERT INTO ea(g,amt,st) SELECT i%3, (i+0.5)::numeric, (ARRAY['new','done','cancel'])[1+i%3]
  FROM generate_series(31,60) i;
DELETE FROM ea WHERE id % 5 = 0;
UPDATE ea SET st='done', amt = amt + 1 WHERE id % 4 = 0;

DO $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM (
    (SELECT k,done_amt,avg_amt,c FROM ea_mv
       EXCEPT SELECT g, SUM(CASE WHEN st='done' THEN amt ELSE 0 END), AVG(COALESCE(amt,0)), COUNT(*)
              FROM ea GROUP BY g)
    UNION ALL
    (SELECT g, SUM(CASE WHEN st='done' THEN amt ELSE 0 END), AVG(COALESCE(amt,0)), COUNT(*)
       FROM ea GROUP BY g
       EXCEPT SELECT k,done_amt,avg_amt,c FROM ea_mv)
  ) x;
  IF n=0 THEN RAISE NOTICE 'SUM(CASE)+AVG(COALESCE) lifecycle correct: PASS';
  ELSE RAISE EXCEPTION 'expression-aggregate lifecycle: FAIL (% diff)', n; END IF;
END $$;
DROP MATERIALIZED VIEW ea_mv;

-- 2. Safety rails — each must be rejected at CREATE.
CREATE OR REPLACE FUNCTION _rej(sql text) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _r WITH (incremental_refresh=true) AS '||sql;
  EXECUTE 'DROP MATERIALIZED VIEW _r';
  RETURN false;   -- created = NOT rejected
EXCEPTION WHEN OTHERS THEN
  RETURN true;    -- rejected
END $$;

DO $$
DECLARE ok bool := true;
BEGIN
  -- volatile inside CASE (numeric SUM, so the float guard does not mask it)
  ok := ok AND _rej('SELECT g, SUM(CASE WHEN random()<0.5 THEN amt ELSE 0 END) s, COUNT(*) c FROM ea GROUP BY g');
  -- stable function (now()) inside CASE
  ok := ok AND _rej('SELECT g, SUM(CASE WHEN ts<now() THEN amt ELSE 0 END) s, COUNT(*) c FROM ea GROUP BY g');
  -- expression arg in a MIN/MAX shape (deparse not wired there)
  ok := ok AND _rej('SELECT g, MIN(amt) mn, SUM(CASE WHEN amt>0 THEN amt ELSE 0 END) s, COUNT(*) c FROM ea GROUP BY g');
  -- expression arg in a HAVING shape (deparse not wired there)
  ok := ok AND _rej('SELECT g, SUM(CASE WHEN amt>0 THEN amt ELSE 0 END) s, COUNT(*) c FROM ea GROUP BY g HAVING COUNT(*)>1');
  IF ok THEN RAISE NOTICE 'unsafe / non-deparse-shape expression args rejected: PASS';
  ELSE RAISE EXCEPTION 'an unsafe expression-aggregate shape was accepted: FAIL'; END IF;
END $$;

-- 3. Control: an immutable expression arg in a plain single-table aggregate is
--    ACCEPTED (proves the rails reject for the right reason, not blanket).
DO $$
BEGIN
  IF NOT _rej('SELECT g, SUM(CASE WHEN amt>5 THEN amt ELSE 0 END) s, COUNT(*) c FROM ea GROUP BY g')
  THEN RAISE NOTICE 'immutable CASE arg accepted (control): PASS';
  ELSE RAISE EXCEPTION 'immutable CASE arg was rejected: FAIL'; END IF;
END $$;

DROP FUNCTION _rej(text);
DROP TABLE ea CASCADE;
\echo ''
\echo '=== expression aggregate arguments test complete ==='
