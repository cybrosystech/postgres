-- DBblue IVM — INNER JOIN aggregate via the deparse delta core.
--
-- An aggregate over a pure INNER JOIN (no outer join, no self-join) builds one
-- delta per source table: the delta for table T swaps only T's range-table
-- entry for its transition-table ENR and leaves the other tables as relations,
-- so ruleutils renders the join naturally.  This requires the ENR FROM item to
-- carry its refname alias (get_rte_alias) so qualified Vars (s.amount) resolve.
--
-- This test proves:
--   1. EQUIVALENCE — a JOIN aggregate is maintained identically (0 diffs vs a
--      live recompute) on the hand path (GUC off) and the deparse path (on),
--      for changes to BOTH joined tables.
--   2. NEW CAPABILITY — SUM(CASE...) over an INNER JOIN is auto-routed to the
--      deparse core under the DEFAULT GUC and maintained correctly.
--   3. SAFETY — expression args over OUTER and SELF joins (deparse not wired)
--      are rejected, not silently mis-built.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: INNER JOIN deparse delta core ==='
\echo ''

DROP TABLE IF EXISTS jp, js CASCADE;
CREATE TABLE jp(id int PRIMARY KEY, categ int);
INSERT INTO jp SELECT g, g%4 FROM generate_series(1,20) g;
CREATE TABLE js(id serial PRIMARY KEY, product_id int, amount numeric, st text);
INSERT INTO js(product_id,amount,st)
  SELECT (g%20)+1, (g%97+1)::numeric, (ARRAY['done','new','cancel'])[1+g%3]
  FROM generate_series(1,400) g;

-- 1. Equivalence: run the same JOIN matview lifecycle on each path.
CREATE OR REPLACE FUNCTION _jrun(deparse bool) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  live text := 'SELECT p.categ AS k, SUM(s.amount) rev, COUNT(*) c, AVG(s.amount) a'
               ' FROM js s JOIN jp p ON p.id=s.product_id GROUP BY p.categ';
  n int;
BEGIN
  EXECUTE 'SET dbblue_ivm_deparse_delta = ' || deparse::text;
  EXECUTE 'CREATE MATERIALIZED VIEW j_mv WITH (incremental_refresh=true) AS ' || live;

  INSERT INTO js(product_id,amount,st)
    SELECT (g%20)+1, (g%97+1)::numeric, 'done' FROM generate_series(401,700) g;
  DELETE FROM js WHERE id % 9 = 0;
  UPDATE js SET amount = amount + 10 WHERE id % 7 = 0;
  INSERT INTO jp VALUES (21,2),(22,3);
  INSERT INTO js(product_id,amount,st) VALUES (21,55,'new'),(22,66,'done');
  DELETE FROM jp WHERE id = 22;                 -- drops matching join rows
  UPDATE jp SET categ = 0 WHERE id = 1;

  EXECUTE format('SELECT count(*) FROM ((SELECT k,rev,c,a FROM j_mv EXCEPT %1$s)
                  UNION ALL (%1$s EXCEPT SELECT k,rev,c,a FROM j_mv)) x', live) INTO n;
  IF n = 0 THEN RAISE NOTICE 'deparse=%: JOIN aggregate lifecycle (both tables) correct: PASS', deparse;
  ELSE RAISE EXCEPTION 'deparse=%: JOIN FAIL (% diff)', deparse, n; END IF;

  EXECUTE 'DROP MATERIALIZED VIEW j_mv';
  -- reset the data set so the second run starts from the same state
  TRUNCATE js; TRUNCATE jp CASCADE;
  INSERT INTO jp SELECT g, g%4 FROM generate_series(1,20) g;
  INSERT INTO js(product_id,amount,st)
    SELECT (g%20)+1, (g%97+1)::numeric, (ARRAY['done','new','cancel'])[1+g%3]
    FROM generate_series(1,400) g;
END $$;

SELECT _jrun(false);   -- hand path
SELECT _jrun(true);    -- deparse path
DROP FUNCTION _jrun(bool);
RESET dbblue_ivm_deparse_delta;

-- 2. SUM(CASE) over an INNER JOIN: auto-routed under the DEFAULT GUC.
SHOW dbblue_ivm_deparse_delta;   -- off
CREATE MATERIALIZED VIEW jcase_mv WITH (incremental_refresh=true) AS
  SELECT p.categ AS k,
         SUM(CASE WHEN s.st='done' THEN s.amount ELSE 0 END) AS done_rev,
         COUNT(*) AS c
  FROM js s JOIN jp p ON p.id = s.product_id
  GROUP BY p.categ;
DO $$
DECLARE s text;
BEGIN
  SELECT ins_sql INTO s FROM pg_dbblue_matview
   WHERE mvrelid='jcase_mv'::regclass AND srctable='js'::regclass;
  IF s LIKE '%CASE%' AND s LIKE '%__mv_newtable%'
  THEN RAISE NOTICE 'SUM(CASE) over JOIN auto-routed to deparse under default GUC: PASS';
  ELSE RAISE EXCEPTION 'SUM(CASE) over JOIN not auto-routed: FAIL (%)', s; END IF;
END $$;
INSERT INTO js(product_id,amount,st) VALUES (3,1000,'done'),(4,2000,'new');
DELETE FROM js WHERE id % 11 = 0;
UPDATE js SET st='done' WHERE id % 6 = 0;
DO $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM (
    (SELECT k,done_rev,c FROM jcase_mv
       EXCEPT SELECT p.categ, SUM(CASE WHEN s.st='done' THEN s.amount ELSE 0 END), COUNT(*)
              FROM js s JOIN jp p ON p.id=s.product_id GROUP BY p.categ)
    UNION ALL
    (SELECT p.categ, SUM(CASE WHEN s.st='done' THEN s.amount ELSE 0 END), COUNT(*)
       FROM js s JOIN jp p ON p.id=s.product_id GROUP BY p.categ
       EXCEPT SELECT k,done_rev,c FROM jcase_mv)
  ) x;
  IF n=0 THEN RAISE NOTICE 'SUM(CASE) over JOIN lifecycle correct: PASS';
  ELSE RAISE EXCEPTION 'SUM(CASE) over JOIN: FAIL (% diff)', n; END IF;
END $$;
DROP MATERIALIZED VIEW jcase_mv;

-- 3. Expression args over OUTER / SELF joins are now SUPPORTED: those shapes use
--    the recompute builders, which render the CASE via the shared grammar.
--    Verify == REFRESH (previously these were rejected as "deparse not wired").
CREATE MATERIALIZED VIEW jlo_mv WITH (incremental_refresh=true) AS
  SELECT p.categ, SUM(CASE WHEN s.st='done' THEN s.amount ELSE 0 END) r, COUNT(*) c
  FROM jp p LEFT JOIN js s ON p.id=s.product_id GROUP BY p.categ;
CREATE MATERIALIZED VIEW jsj_mv WITH (incremental_refresh=true) AS
  SELECT a.categ, SUM(CASE WHEN a.id>b.id THEN 1 ELSE 0 END) r, COUNT(*) c
  FROM jp a JOIN jp b ON a.categ=b.categ GROUP BY a.categ;
INSERT INTO js(product_id,st,amount) VALUES (1,'done',20),(2,'open',3);
DELETE FROM js WHERE st='open' AND amount=5;
UPDATE js SET st='done' WHERE amount=3;
CREATE MATERIALIZED VIEW jlo_n AS
  SELECT p.categ, SUM(CASE WHEN s.st='done' THEN s.amount ELSE 0 END) r, COUNT(*) c
  FROM jp p LEFT JOIN js s ON p.id=s.product_id GROUP BY p.categ;
CREATE MATERIALIZED VIEW jsj_n AS
  SELECT a.categ, SUM(CASE WHEN a.id>b.id THEN 1 ELSE 0 END) r, COUNT(*) c
  FROM jp a JOIN jp b ON a.categ=b.categ GROUP BY a.categ;
DO $$
DECLARE d1 int; d2 int;
BEGIN
  SELECT count(*) INTO d1 FROM ((SELECT categ,r,c FROM jlo_mv EXCEPT SELECT categ,r,c FROM jlo_n)
    UNION ALL (SELECT categ,r,c FROM jlo_n EXCEPT SELECT categ,r,c FROM jlo_mv)) z;
  SELECT count(*) INTO d2 FROM ((SELECT categ,r,c FROM jsj_mv EXCEPT SELECT categ,r,c FROM jsj_n)
    UNION ALL (SELECT categ,r,c FROM jsj_n EXCEPT SELECT categ,r,c FROM jsj_mv)) z;
  IF d1=0 AND d2=0 THEN RAISE NOTICE 'CASE args over LEFT JOIN and self-join == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'CASE-arg LEFT/self join diverged (left=%, self=%)', d1, d2; END IF;
END $$;
DROP MATERIALIZED VIEW jlo_mv; DROP MATERIALIZED VIEW jsj_mv;
DROP MATERIALIZED VIEW jlo_n; DROP MATERIALIZED VIEW jsj_n;

DROP TABLE jp, js CASCADE;
\echo ''
\echo '=== INNER JOIN deparse delta core test complete ==='
