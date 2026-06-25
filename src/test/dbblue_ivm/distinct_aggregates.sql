-- DBblue IVM — DISTINCT aggregates (COUNT(DISTINCT x), SUM(DISTINCT x), …).
--
-- A DISTINCT aggregate can't be maintained by a ±1 per-row delta (it can't know
-- whether a value is new to, or the last occurrence in, its group).  For a
-- single-table aggregate the engine maintains it by RECOMPUTING each affected
-- group from the live table (incr_build_recompute_apply_sql) — exactly what a
-- full REFRESH yields for those groups — correct for every aggregate in the
-- matview (regular ones alongside the DISTINCT ones), and idempotent (INSERT and
-- DELETE both write the absolute recomputed value, so one statement doing both
-- composes).  Serialized on the matview-level lock; NULL group keys are excluded
-- (recompute matches keys with =/IN), like the other recompute shapes.
--
-- Checked == a full REFRESH after the full lifecycle: a brand-new distinct value,
-- a duplicate (no change), deleting a non-last occurrence (no change), deleting
-- the last occurrence (count drops / group vanishes), an UPDATE shifting a value,
-- and a new group.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: DISTINCT aggregates ==='
\echo ''

DROP TABLE IF EXISTS da CASCADE;
CREATE TABLE da(id serial primary key, g int, v int, w int);
INSERT INTO da(g,v,w) VALUES (1,10,1),(1,10,2),(1,20,3),(2,5,4),(2,5,5);
CREATE MATERIALIZED VIEW da_i WITH (incremental_refresh=true) AS
  SELECT g, COUNT(DISTINCT v) dv, COUNT(*) c, SUM(w) sw,
         COUNT(DISTINCT w) dw, SUM(DISTINCT v) sdv, AVG(w) aw
  FROM da GROUP BY g;
CREATE MATERIALIZED VIEW da_n AS
  SELECT g, COUNT(DISTINCT v) dv, COUNT(*) c, SUM(w) sw,
         COUNT(DISTINCT w) dw, SUM(DISTINCT v) sdv, AVG(w) aw
  FROM da GROUP BY g;

INSERT INTO da(g,v,w) VALUES (1,30,6);      -- new distinct value 30 -> dv 2->3
INSERT INTO da(g,v,w) VALUES (1,10,7);      -- duplicate value 10 -> dv unchanged
DELETE FROM da WHERE g=1 AND v=10 AND w=1;  -- 10 still present (w=2,7) -> dv unchanged
DELETE FROM da WHERE g=2 AND v=5 AND w=4;   -- 5 still present (w=5) -> dv unchanged
DELETE FROM da WHERE g=2 AND v=5 AND w=5;   -- last row of group 2 -> group vanishes
UPDATE da SET v=99 WHERE g=1 AND v=20;      -- 20 -> 99 distinct shift
INSERT INTO da(g,v,w) VALUES (3,7,8),(3,7,9); -- new group, one distinct v

REFRESH MATERIALIZED VIEW da_n;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT g,dv,c,sw,dw,sdv,aw FROM da_i EXCEPT SELECT g,dv,c,sw,dw,sdv,aw FROM da_n)
    UNION ALL (SELECT g,dv,c,sw,dw,sdv,aw FROM da_n EXCEPT SELECT g,dv,c,sw,dw,sdv,aw FROM da_i)) d;
  IF ndiff=0 THEN RAISE NOTICE 'DISTINCT aggregates == REFRESH (full lifecycle): PASS';
  ELSE RAISE EXCEPTION 'DISTINCT aggregates: FAIL (% diff)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW da_i; DROP MATERIALIZED VIEW da_n; DROP TABLE da CASCADE;

-- Idempotent recompute: one statement that both DELETEs and INSERTs must stay
-- correct (both write the absolute recomputed group value, no double-count).
DROP TABLE IF EXISTS da2 CASCADE;
CREATE TABLE da2(g int, v int);
INSERT INTO da2 VALUES (1,1),(1,2),(1,2),(2,9);
CREATE MATERIALIZED VIEW da2_i WITH (incremental_refresh=true) AS
  SELECT g, COUNT(DISTINCT v) dv, COUNT(*) c FROM da2 GROUP BY g;
WITH d AS (DELETE FROM da2 WHERE v=2 RETURNING v),
     i AS (INSERT INTO da2 VALUES (1,3),(1,1),(2,9) RETURNING v)
SELECT (SELECT count(*) FROM d) + (SELECT count(*) FROM i);
CREATE MATERIALIZED VIEW da2_n AS
  SELECT g, COUNT(DISTINCT v) dv, COUNT(*) c FROM da2 GROUP BY g;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT g,dv,c FROM da2_i EXCEPT SELECT g,dv,c FROM da2_n)
    UNION ALL (SELECT g,dv,c FROM da2_n EXCEPT SELECT g,dv,c FROM da2_i)) d;
  IF ndiff=0 THEN RAISE NOTICE 'DISTINCT under combined DELETE+INSERT == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'DISTINCT combined DELETE+INSERT: FAIL (% diff)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW da2_i; DROP MATERIALIZED VIEW da2_n; DROP TABLE da2 CASCADE;

-- COUNT(DISTINCT) over an INNER JOIN — recompute per source table.  Includes a
-- far-table group-key change (the join-order builder must keep the connecting
-- condition), plus fact insert/delete.
DROP TABLE IF EXISTS da_o CASCADE; DROP TABLE IF EXISTS da_l CASCADE;
CREATE TABLE da_o(id int primary key, region text);
CREATE TABLE da_l(id serial primary key, oid int, cust int, amt numeric);
INSERT INTO da_o VALUES (1,'E'),(2,'W');
INSERT INTO da_l(oid,cust,amt) VALUES (1,100,5),(1,100,8),(1,200,3),(2,300,7);
CREATE MATERIALIZED VIEW da_ji WITH (incremental_refresh=true) AS
  SELECT r.region, COUNT(DISTINCT l.cust) dc, COUNT(*) c, SUM(l.amt) s
  FROM da_l l JOIN da_o r ON r.id=l.oid GROUP BY r.region;
CREATE MATERIALIZED VIEW da_jn AS
  SELECT r.region, COUNT(DISTINCT l.cust) dc, COUNT(*) c, SUM(l.amt) s
  FROM da_l l JOIN da_o r ON r.id=l.oid GROUP BY r.region;
INSERT INTO da_l(oid,cust,amt) VALUES (1,400,1),(2,300,9);  -- new cust in E; dup in W
DELETE FROM da_l WHERE oid=1 AND cust=100 AND amt=5;        -- 100 still present in E
UPDATE da_o SET region='E' WHERE id=2;                      -- far-table key change: W -> E
REFRESH MATERIALIZED VIEW da_jn;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT region,dc,c,s FROM da_ji EXCEPT SELECT region,dc,c,s FROM da_jn)
    UNION ALL (SELECT region,dc,c,s FROM da_jn EXCEPT SELECT region,dc,c,s FROM da_ji)) d;
  IF ndiff=0 THEN RAISE NOTICE 'COUNT(DISTINCT) over INNER JOIN == REFRESH (far-table key change): PASS';
  ELSE RAISE EXCEPTION 'COUNT(DISTINCT) over JOIN: FAIL (% diff)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW da_ji; DROP MATERIALIZED VIEW da_jn; DROP TABLE da_l, da_o CASCADE;
\echo ''
\echo '=== DISTINCT aggregates test complete ==='
