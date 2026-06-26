-- DBblue IVM — recompute-only aggregates: STDDEV / VARIANCE / BOOL_AND / BOOL_OR.
--
-- These can't be maintained additively, so a matview containing one is routed to
-- the recompute path (recompute each affected group from the live table(s) — the
-- same path COUNT(DISTINCT) uses).  Single-table and INNER JOIN are supported;
-- args may be any IMMUTABLE expression (the shared grammar now renders CASE /
-- COALESCE / arithmetic).  Checked == a full REFRESH over the lifecycle.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: STDDEV / VARIANCE / BOOL_AND / BOOL_OR ==='
\echo ''

-- single table, mixed with regular aggregates and a CASE arg
DROP TABLE IF EXISTS sb CASCADE;
CREATE TABLE sb(id serial primary key, g int, v numeric, ok bool);
INSERT INTO sb(g,v,ok) VALUES (1,10,true),(1,20,true),(1,30,false),(2,5,true),(2,NULL,true);
CREATE MATERIALIZED VIEW sb_i WITH (incremental_refresh=true) AS
  SELECT g, stddev(v) sd, var_pop(v) vp, bool_and(ok) ba, bool_or(ok) bo,
         stddev(CASE WHEN ok THEN v ELSE 0 END) sdc, COUNT(*) c
  FROM sb GROUP BY g;
CREATE MATERIALIZED VIEW sb_n AS
  SELECT g, stddev(v) sd, var_pop(v) vp, bool_and(ok) ba, bool_or(ok) bo,
         stddev(CASE WHEN ok THEN v ELSE 0 END) sdc, COUNT(*) c
  FROM sb GROUP BY g;
INSERT INTO sb(g,v,ok) VALUES (1,40,true),(2,15,false),(3,7,true);
DELETE FROM sb WHERE g=1 AND v=10;
UPDATE sb SET ok=true WHERE g=1 AND v=30;
UPDATE sb SET v=25 WHERE g=2 AND v=5;
REFRESH MATERIALIZED VIEW sb_n;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT g,sd,vp,ba,bo,sdc,c FROM sb_i EXCEPT SELECT g,sd,vp,ba,bo,sdc,c FROM sb_n)
    UNION ALL (SELECT g,sd,vp,ba,bo,sdc,c FROM sb_n EXCEPT SELECT g,sd,vp,ba,bo,sdc,c FROM sb_i)) d;
  IF ndiff=0 THEN RAISE NOTICE 'single-table STDDEV/VAR/BOOL (+CASE arg) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'STDDEV/VAR/BOOL single-table: FAIL (% diff)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW sb_i; DROP MATERIALIZED VIEW sb_n; DROP TABLE sb CASCADE;

-- over an INNER JOIN
DROP TABLE IF EXISTS sb_l CASCADE; DROP TABLE IF EXISTS sb_d CASCADE;
CREATE TABLE sb_d(id int primary key, region text);
CREATE TABLE sb_l(id serial primary key, did int, amt numeric, ok bool);
INSERT INTO sb_d VALUES (1,'E'),(2,'W');
INSERT INTO sb_l(did,amt,ok) VALUES (1,10,true),(1,20,false),(2,5,true);
CREATE MATERIALIZED VIEW sbj_i WITH (incremental_refresh=true) AS
  SELECT d.region, stddev(l.amt) sd, bool_or(l.ok) bo, COUNT(*) c
  FROM sb_l l JOIN sb_d d ON d.id=l.did GROUP BY d.region;
CREATE MATERIALIZED VIEW sbj_n AS
  SELECT d.region, stddev(l.amt) sd, bool_or(l.ok) bo, COUNT(*) c
  FROM sb_l l JOIN sb_d d ON d.id=l.did GROUP BY d.region;
INSERT INTO sb_l(did,amt,ok) VALUES (1,30,true),(2,50,false);
DELETE FROM sb_l WHERE amt=5;
UPDATE sb_d SET region='E' WHERE id=2;   -- far-table key change
REFRESH MATERIALIZED VIEW sbj_n;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT region,sd,bo,c FROM sbj_i EXCEPT SELECT region,sd,bo,c FROM sbj_n)
    UNION ALL (SELECT region,sd,bo,c FROM sbj_n EXCEPT SELECT region,sd,bo,c FROM sbj_i)) d;
  IF ndiff=0 THEN RAISE NOTICE 'INNER JOIN STDDEV/BOOL == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'STDDEV/BOOL over JOIN: FAIL (% diff)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW sbj_i; DROP MATERIALIZED VIEW sbj_n; DROP TABLE sb_l, sb_d CASCADE;
\echo ''
\echo '=== STDDEV / VARIANCE / BOOL_AND / BOOL_OR test complete ==='
