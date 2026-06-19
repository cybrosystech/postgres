-- DBblue IVM — regressions found by the deparse-core correctness audit.
-- Each case compares the incremental matview against a full REFRESH (the
-- ground-truth oracle), or asserts a clean rejection.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: audit regression tests ==='
\echo ''

-- BUG 1: MIN/MAX coexisting with SUM/COUNT(arg)/AVG corrupted SUM/COUNT to
-- NULL/0 when an aggregate argument transitioned NULL->value (the MIN/MAX INS
-- used plain "+" instead of NULL-safe accumulation, and the DEL subtracted
-- COUNT(*) from COUNT(arg) columns).  Verify the full NULL-arg lifecycle equals
-- a REFRESH, on both the hand and deparse paths.
CREATE OR REPLACE FUNCTION _b1(deparse bool) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
  EXECUTE 'SET dbblue_ivm_deparse_delta = '||deparse::text;
  DROP TABLE IF EXISTS b1 CASCADE;
  CREATE TABLE b1 (id int primary key, g int, v numeric);
  INSERT INTO b1 VALUES (1,1,NULL);
  CREATE MATERIALIZED VIEW b1i WITH (incremental_refresh=true) AS
    SELECT g, sum(v) s, count(v) c, avg(v) a, min(v) mn, max(v) mx FROM b1 GROUP BY g;
  CREATE MATERIALIZED VIEW b1n AS
    SELECT g, sum(v) s, count(v) c, avg(v) a, min(v) mn, max(v) mx FROM b1 GROUP BY g;
  UPDATE b1 SET v=999 WHERE id=1;                 -- NULL -> value (MIN/MAX present)
  INSERT INTO b1 VALUES (2,1,10),(3,1,NULL),(4,2,5),(5,2,NULL);
  UPDATE b1 SET v=NULL WHERE id=2;                -- value -> NULL
  UPDATE b1 SET v=7 WHERE id=3;
  DELETE FROM b1 WHERE id=4;                       -- group 2: remove its only non-null
  UPDATE b1 SET v=3 WHERE id=5;                    -- group 2: re-populate
  REFRESH MATERIALIZED VIEW b1n;
  SELECT count(*) INTO n FROM (
    (SELECT g,s,c,a,mn,mx FROM b1i EXCEPT SELECT g,s,c,a,mn,mx FROM b1n)
    UNION ALL (SELECT g,s,c,a,mn,mx FROM b1n EXCEPT SELECT g,s,c,a,mn,mx FROM b1i)) d;
  DROP MATERIALIZED VIEW b1i; DROP MATERIALIZED VIEW b1n; DROP TABLE b1 CASCADE;
  RETURN n;
END $$;
DO $$
BEGIN
  IF _b1(false)=0 AND _b1(true)=0
  THEN RAISE NOTICE 'BUG1 MIN/MAX + SUM/COUNT/AVG NULL-arg lifecycle == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUG1: incremental diverged from REFRESH'; END IF;
END $$;
DROP FUNCTION _b1(bool);
RESET dbblue_ivm_deparse_delta;

-- BUG 2: a single-row INSERT into the deepest table of a 3-table INNER JOIN
-- aggregate was broadcast to other groups (hand join-delta path).  Default GUC.
DROP TABLE IF EXISTS j_cu,j_or,j_li CASCADE;
CREATE TABLE j_cu (id int primary key, name text);
CREATE TABLE j_or (id int primary key, cu_id int);
CREATE TABLE j_li (id int primary key, or_id int, price numeric);
INSERT INTO j_cu VALUES (1,'A'),(2,'B'),(3,'C');
INSERT INTO j_or VALUES (10,1),(20,2),(30,3);
INSERT INTO j_li VALUES (100,10,5),(200,20,7),(300,30,11);
CREATE MATERIALIZED VIEW j_inc WITH (incremental_refresh=true) AS
  SELECT a.name cust, sum(c.price) rev, count(*) lines
  FROM j_cu a JOIN j_or b ON a.id=b.cu_id JOIN j_li c ON b.id=c.or_id GROUP BY a.name;
CREATE MATERIALIZED VIEW j_norm AS
  SELECT a.name cust, sum(c.price) rev, count(*) lines
  FROM j_cu a JOIN j_or b ON a.id=b.cu_id JOIN j_li c ON b.id=c.or_id GROUP BY a.name;
INSERT INTO j_li VALUES (201,20,100);          -- only B's order
DELETE FROM j_li WHERE id=100;                  -- remove an A line
INSERT INTO j_or VALUES (40,1); INSERT INTO j_li VALUES (400,40,40);  -- new order for A
UPDATE j_li SET price=price+1 WHERE id=300;     -- C
REFRESH MATERIALIZED VIEW j_norm;
DO $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM (
    (SELECT cust,rev,lines FROM j_inc EXCEPT SELECT cust,rev,lines FROM j_norm)
    UNION ALL (SELECT cust,rev,lines FROM j_norm EXCEPT SELECT cust,rev,lines FROM j_inc)) d;
  IF n=0 THEN RAISE NOTICE 'BUG2 3-table INNER JOIN single-row delta == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUG2: 3-table join diverged (% rows)', n; END IF;
END $$;
DROP MATERIALIZED VIEW j_inc; DROP MATERIALIZED VIEW j_norm; DROP TABLE j_cu,j_or,j_li CASCADE;

-- BUG 3: LEFT/RIGHT/FULL self-join must be rejected cleanly (it used to leak an
-- internal unique-constraint violation on the engine catalog).
DROP TABLE IF EXISTS emp CASCADE;
CREATE TABLE emp(id int primary key, mgr int, sal int);
INSERT INTO emp VALUES (1,NULL,1000),(2,1,500),(3,1,600);
DO $$
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _ss WITH (incremental_refresh=true) AS
           SELECT e.mgr mgrid, count(*) cnt
           FROM emp e LEFT JOIN emp m ON e.mgr=m.id GROUP BY e.mgr';
  RAISE EXCEPTION 'BUG3: LEFT self-join was accepted (should be rejected)';
EXCEPTION
  WHEN feature_not_supported THEN RAISE NOTICE 'BUG3 LEFT self-join rejected cleanly: PASS';
  WHEN others THEN RAISE EXCEPTION 'BUG3: wrong error % (%) — expected feature_not_supported', SQLSTATE, SQLERRM;
END $$;
DROP TABLE emp CASCADE;

-- BUG A (audit round 2): a row-level (no GROUP BY) matview keeps duplicate rows;
-- a DELETE of one source row used to remove ALL value-identical output copies.
-- Verify the multiset matches a full REFRESH after duplicate-touching DML, for
-- a single table and an inner join.
DROP TABLE IF EXISTS rl CASCADE;
CREATE TABLE rl(id int, name text, val int);
INSERT INTO rl VALUES (1,'a',10),(1,'a',10),(1,'a',10),(2,'b',20);
CREATE MATERIALIZED VIEW rli WITH (incremental_refresh=true) AS SELECT id,name,val FROM rl;
CREATE MATERIALIZED VIEW rln AS SELECT id,name,val FROM rl;
DELETE FROM rl WHERE ctid IN (SELECT ctid FROM rl WHERE id=1 LIMIT 1);   -- one of three copies
INSERT INTO rl VALUES (2,'b',20);
DELETE FROM rl WHERE ctid IN (SELECT ctid FROM rl WHERE id=2 LIMIT 1);
REFRESH MATERIALIZED VIEW rln;
DO $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM (
    (SELECT id,name,val FROM rli EXCEPT ALL SELECT id,name,val FROM rln)
    UNION ALL (SELECT id,name,val FROM rln EXCEPT ALL SELECT id,name,val FROM rli)) d;
  IF n=0 THEN RAISE NOTICE 'BUGA row-level duplicate multiplicity == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUGA: row-level multiset diverged (% rows)', n; END IF;
END $$;
DROP MATERIALIZED VIEW rli; DROP MATERIALIZED VIEW rln; DROP TABLE rl CASCADE;
\echo ''
\echo '=== audit regression tests complete ==='
