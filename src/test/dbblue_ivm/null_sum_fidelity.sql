-- DBblue IVM — all-NULL SUM shows SQL-exact NULL (not 0).
--
-- SUM(x) over a group whose contributing values are all NULL is SQL NULL, not 0.
-- Incrementally this used to leave 0 once a group's last non-NULL input was
-- removed (run - delta = 0).  A hidden per-column non-null counter
-- (__mv_sumcnt_<col>) now lets the visible SUM be rendered as
-- (sumcnt = 0 ? NULL : running_sum), and the running sum recovers from NULL
-- when a non-NULL input returns.  Verified == full REFRESH for the shared-shell
-- shapes (single-table, INNER JOIN, HAVING) on both the hand and deparse paths.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: all-NULL SUM fidelity ==='
\echo ''

CREATE OR REPLACE FUNCTION _ns(deparse bool) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
  EXECUTE 'SET dbblue_ivm_deparse_delta = ' || deparse::text;
  DROP TABLE IF EXISTS nsf CASCADE;
  CREATE TABLE nsf(id int primary key, g int, amt numeric);
  INSERT INTO nsf VALUES (1,1,5),(2,1,NULL),(3,2,8),(4,3,NULL);
  CREATE MATERIALIZED VIEW nsi WITH (incremental_refresh=true) AS
    SELECT g, SUM(amt) s, COUNT(*) c, COUNT(amt) ca FROM nsf GROUP BY g;
  CREATE MATERIALIZED VIEW nsn AS
    SELECT g, SUM(amt) s, COUNT(*) c, COUNT(amt) ca FROM nsf GROUP BY g;
  DELETE FROM nsf WHERE id=1;            -- g=1: last non-NULL removed -> SUM NULL
  UPDATE nsf SET amt=NULL WHERE id=3;    -- g=2: only value -> NULL -> SUM NULL
  INSERT INTO nsf VALUES (5,1,10);       -- g=1: non-NULL returns -> SUM recovers to 10
  INSERT INTO nsf VALUES (6,3,2);        -- g=3: was all-NULL, now 2
  REFRESH MATERIALIZED VIEW nsn;
  SELECT count(*) INTO n FROM (
    (SELECT g,s,c,ca FROM nsi EXCEPT SELECT g,s,c,ca FROM nsn)
    UNION ALL (SELECT g,s,c,ca FROM nsn EXCEPT SELECT g,s,c,ca FROM nsi)) d;
  DROP MATERIALIZED VIEW nsi; DROP MATERIALIZED VIEW nsn; DROP TABLE nsf CASCADE;
  RETURN n;
END $$;
DO $$
BEGIN
  IF _ns(false)=0 AND _ns(true)=0
  THEN RAISE NOTICE 'all-NULL SUM == REFRESH (NULL, and recovers), both paths: PASS';
  ELSE RAISE EXCEPTION 'all-NULL SUM fidelity: FAIL'; END IF;
END $$;
DROP FUNCTION _ns(bool);
RESET dbblue_ivm_deparse_delta;

-- JOIN + HAVING with a nullable SUM that empties for a group
DROP TABLE IF EXISTS hp, hs CASCADE;
CREATE TABLE hp(id int primary key, c int);
CREATE TABLE hs(id serial primary key, pid int, amt numeric);
INSERT INTO hp VALUES (1,10),(2,20);
INSERT INTO hs(pid,amt) VALUES (1,5),(1,NULL),(2,NULL);
CREATE MATERIALIZED VIEW hi WITH (incremental_refresh=true) AS
  SELECT p.c k, SUM(s.amt) s, COUNT(*) n FROM hs s JOIN hp p ON p.id=s.pid GROUP BY p.c;
CREATE MATERIALIZED VIEW hn AS
  SELECT p.c k, SUM(s.amt) s, COUNT(*) n FROM hs s JOIN hp p ON p.id=s.pid GROUP BY p.c;
DELETE FROM hs WHERE amt=5;             -- k=10: only NULL left -> SUM NULL
REFRESH MATERIALIZED VIEW hn;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT k,s,n FROM hi EXCEPT SELECT k,s,n FROM hn)
    UNION ALL (SELECT k,s,n FROM hn EXCEPT SELECT k,s,n FROM hi)) d;
  IF ndiff=0 THEN RAISE NOTICE 'JOIN all-NULL SUM == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'JOIN all-NULL SUM: FAIL (% diff)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW hi; DROP MATERIALIZED VIEW hn; DROP TABLE hp, hs CASCADE;
\echo ''
\echo '=== all-NULL SUM fidelity test complete ==='
