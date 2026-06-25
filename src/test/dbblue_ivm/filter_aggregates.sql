-- DBblue IVM — aggregate FILTER (WHERE …).
--
-- agg(x) FILTER (WHERE c) is the exact equivalent of agg(CASE WHEN c THEN x END);
-- the engine rewrites it to that form at CREATE (MatviewIncrRewriteAggFilters)
-- before eligibility, so the deparse delta core maintains it with no
-- delta-builder changes.  Supported for SUM / COUNT(*) / COUNT(col) / AVG over
-- single-table and INNER JOIN aggregates (incl. HAVING).  MIN/MAX FILTER stays
-- rejected (the hand MIN/MAX builder can't render CASE — see
-- unsupported_aggregates.sql).
--
-- Every shape is checked == a full REFRESH (the ground-truth oracle) after a mix
-- of INSERT/DELETE/UPDATE (including UPDATEs that move a row across the filter),
-- on BOTH the deparse-default and GUC-off paths (FILTER forces deparse either way).
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: aggregate FILTER ==='
\echo ''

CREATE OR REPLACE FUNCTION _fa(deparse bool) RETURNS int LANGUAGE plpgsql AS $$
DECLARE ndiff int := 0; t int;
BEGIN
  EXECUTE 'SET dbblue_ivm_deparse_delta = ' || deparse::text;

  ----------------------------------------------------------------- single table
  DROP TABLE IF EXISTS fa_o CASCADE;
  CREATE TABLE fa_o(id serial primary key, g int, status text, amt numeric);
  INSERT INTO fa_o(g,status,amt) VALUES
    (1,'done',10),(1,'open',5),(1,'done',NULL),(2,'done',8),(2,'open',3),(3,'open',NULL);
  CREATE MATERIALIZED VIEW fa_i WITH (incremental_refresh=true) AS
    SELECT g,
           COUNT(*)   FILTER (WHERE status='done') ndone,
           COUNT(amt) FILTER (WHERE status='done') namt,
           SUM(amt)   FILTER (WHERE status='done') sdone,
           AVG(amt)   FILTER (WHERE status='open') aopen,
           COUNT(*) tot
    FROM fa_o GROUP BY g;
  CREATE MATERIALIZED VIEW fa_n AS
    SELECT g,
           COUNT(*)   FILTER (WHERE status='done') ndone,
           COUNT(amt) FILTER (WHERE status='done') namt,
           SUM(amt)   FILTER (WHERE status='done') sdone,
           AVG(amt)   FILTER (WHERE status='open') aopen,
           COUNT(*) tot
    FROM fa_o GROUP BY g;
  INSERT INTO fa_o(g,status,amt) VALUES (1,'done',100),(4,'done',7),(2,'open',NULL);
  DELETE FROM fa_o WHERE status='open' AND amt=5;
  UPDATE fa_o SET status='done' WHERE g=2 AND amt=3;       -- row moves across the filter
  UPDATE fa_o SET amt=20 WHERE g=1 AND status='done' AND amt=10;
  REFRESH MATERIALIZED VIEW fa_n;
  SELECT count(*) INTO t FROM (
    (SELECT g,ndone,namt,sdone,aopen,tot FROM fa_i EXCEPT SELECT g,ndone,namt,sdone,aopen,tot FROM fa_n)
    UNION ALL (SELECT g,ndone,namt,sdone,aopen,tot FROM fa_n EXCEPT SELECT g,ndone,namt,sdone,aopen,tot FROM fa_i)) d;
  ndiff := ndiff + t;
  DROP MATERIALIZED VIEW fa_i; DROP MATERIALIZED VIEW fa_n;

  ------------------------------------------------- INNER JOIN + FILTER + HAVING
  DROP TABLE IF EXISTS fa_l CASCADE; DROP TABLE IF EXISTS fa_d CASCADE;
  CREATE TABLE fa_d(id int primary key, region text);
  CREATE TABLE fa_l(id serial primary key, oid int, kind text, amt numeric);
  INSERT INTO fa_d VALUES (1,'E'),(2,'W');
  INSERT INTO fa_l(oid,kind,amt) VALUES (1,'a',10),(1,'b',5),(2,'a',8),(2,'b',NULL);
  CREATE MATERIALIZED VIEW fa_i WITH (incremental_refresh=true) AS
    SELECT r.region,
           SUM(l.amt) FILTER (WHERE l.kind='a') sa,
           COUNT(*)   FILTER (WHERE l.kind='b') nb
    FROM fa_l l JOIN fa_d r ON r.id=l.oid GROUP BY r.region
    HAVING SUM(l.amt) FILTER (WHERE l.kind='a') > 0;
  CREATE MATERIALIZED VIEW fa_n AS
    SELECT r.region,
           SUM(l.amt) FILTER (WHERE l.kind='a') sa,
           COUNT(*)   FILTER (WHERE l.kind='b') nb
    FROM fa_l l JOIN fa_d r ON r.id=l.oid GROUP BY r.region
    HAVING SUM(l.amt) FILTER (WHERE l.kind='a') > 0;
  INSERT INTO fa_l(oid,kind,amt) VALUES (2,'a',50),(1,'b',1);
  DELETE FROM fa_l WHERE kind='a' AND amt=10;               -- can drop a group below HAVING
  UPDATE fa_l SET kind='b' WHERE oid=2 AND amt=8;           -- move across the filter
  REFRESH MATERIALIZED VIEW fa_n;
  SELECT count(*) INTO t FROM (
    (SELECT region,sa,nb FROM fa_i EXCEPT SELECT region,sa,nb FROM fa_n)
    UNION ALL (SELECT region,sa,nb FROM fa_n EXCEPT SELECT region,sa,nb FROM fa_i)) d;
  ndiff := ndiff + t;
  -- HAVING matview: original name is a VIEW over the hidden base
  DROP VIEW fa_i; DROP MATERIALIZED VIEW fa_n;

  DROP TABLE fa_o CASCADE; DROP TABLE fa_l CASCADE; DROP TABLE fa_d CASCADE;
  RETURN ndiff;
END $$;

DO $$
BEGIN
  IF _fa(true) = 0 AND _fa(false) = 0 THEN
    RAISE NOTICE 'aggregate FILTER == REFRESH on both paths (SUM/COUNT/AVG, single/JOIN/HAVING): PASS';
  ELSE
    RAISE EXCEPTION 'aggregate FILTER fidelity: FAIL';
  END IF;
END $$;
DROP FUNCTION _fa(bool);
RESET dbblue_ivm_deparse_delta;
\echo ''
\echo '=== aggregate FILTER test complete ==='
