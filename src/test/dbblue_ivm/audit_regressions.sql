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

-- BUG 3 (historical): LEFT/RIGHT self-join used to leak an internal
-- unique-constraint violation on the engine catalog and was then rejected
-- cleanly.  The shape is now SUPPORTED (self-outer recompute path, one combined
-- catalog row per OID), so this case asserts acceptance + correctness instead:
-- the incremental matview must match a full REFRESH after deltas.  A FULL
-- self-join remains rejected cleanly (checked in distinct_outer_join Case 16).
DROP TABLE IF EXISTS emp CASCADE;
CREATE TABLE emp(id int primary key, mgr int, sal int);
INSERT INTO emp VALUES (1,NULL,1000),(2,1,500),(3,1,600);
CREATE MATERIALIZED VIEW _ss WITH (incremental_refresh=true) AS
  SELECT e.mgr mgrid, count(*) cnt
  FROM emp e LEFT JOIN emp m ON e.mgr=m.id GROUP BY e.mgr;
CREATE MATERIALIZED VIEW _sn AS
  SELECT e.mgr mgrid, count(*) cnt
  FROM emp e LEFT JOIN emp m ON e.mgr=m.id GROUP BY e.mgr;
INSERT INTO emp VALUES (4,3,700);   -- new report under 3
DELETE FROM emp WHERE id=2;         -- shrink group mgr=1
UPDATE emp SET mgr=NULL WHERE id=3; -- 3 becomes top-level → moves to NULL group
REFRESH MATERIALIZED VIEW _sn;
DO $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM (
    (SELECT mgrid,cnt FROM _ss EXCEPT SELECT mgrid,cnt FROM _sn)
    UNION ALL (SELECT mgrid,cnt FROM _sn EXCEPT SELECT mgrid,cnt FROM _ss)) d;
  IF n=0 THEN RAISE NOTICE 'BUG3 self LEFT join (now supported) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUG3: self LEFT join diverged from REFRESH (% rows)', n; END IF;
END $$;
DROP MATERIALIZED VIEW _ss; DROP MATERIALIZED VIEW _sn;
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

-- BUG D (audit round 2): a self-join aggregate matview used two per-role delta
-- arms that double-counted the delta-joins-delta overlap and aborted the user's
-- write with "ON CONFLICT ... cannot affect row a second time" on self/mutually
-- referential DML.  Now affected groups are recomputed in one statement: writes
-- must never abort, and the result must equal a full REFRESH.
DROP TABLE IF EXISTS emp CASCADE;
CREATE TABLE emp(id int primary key, mgr int, sal int);
INSERT INTO emp VALUES (1,NULL,100),(2,1,50),(3,1,60),(4,2,40);
CREATE MATERIALIZED VIEW ei WITH (incremental_refresh=true) AS
  SELECT m.id mgr_id, count(*) nreports, sum(e.sal) rep_sal, avg(e.sal) avg_sal
  FROM emp e JOIN emp m ON e.mgr=m.id GROUP BY m.id;
CREATE MATERIALIZED VIEW en AS
  SELECT m.id mgr_id, count(*) nreports, sum(e.sal) rep_sal, avg(e.sal) avg_sal
  FROM emp e JOIN emp m ON e.mgr=m.id GROUP BY m.id;
DO $$
BEGIN
  INSERT INTO emp VALUES (5,5,77);            -- self-referential
  INSERT INTO emp VALUES (6,7,25),(7,6,15);   -- mutually referential, one statement
  UPDATE emp SET mgr=2 WHERE id=2;            -- existing row becomes self-referential
  INSERT INTO emp VALUES (8,1,90); DELETE FROM emp WHERE id=4; UPDATE emp SET sal=sal+5 WHERE id=3;
EXCEPTION WHEN OTHERS THEN
  RAISE EXCEPTION 'BUGD: a self-join base-table write was aborted (% / %)', SQLSTATE, SQLERRM;
END $$;
REFRESH MATERIALIZED VIEW en;
DO $$
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM (
    (SELECT mgr_id,nreports,rep_sal,avg_sal FROM ei EXCEPT SELECT mgr_id,nreports,rep_sal,avg_sal FROM en)
    UNION ALL (SELECT mgr_id,nreports,rep_sal,avg_sal FROM en EXCEPT SELECT mgr_id,nreports,rep_sal,avg_sal FROM ei)) d;
  IF n=0 THEN RAISE NOTICE 'BUGD self-join (incl. self/mutually referential) == REFRESH, no write aborted: PASS';
  ELSE RAISE EXCEPTION 'BUGD: self-join diverged from REFRESH (% rows)', n; END IF;
END $$;
DROP MATERIALIZED VIEW ei; DROP MATERIALIZED VIEW en; DROP TABLE emp CASCADE;

-- BUG B (audit round 2): UNION ALL kept duplicates per its definition, but the
-- matview deduped into one row per value + __mv_count__, so it showed 1 where a
-- REFRESH showed N.  It is now maintained as a plain multiset; verify the
-- multiset equals a full REFRESH after cross-branch duplicate DML.
DROP TABLE IF EXISTS ua, ub CASCADE;
CREATE TABLE ua(id int, val text, n int);
CREATE TABLE ub(id int, val text, n int);
INSERT INTO ua VALUES (1,'y',20),(2,'y',20),(3,'y',20);
INSERT INTO ub VALUES (4,'y',20),(5,'z',5);
CREATE MATERIALIZED VIEW ui WITH (incremental_refresh=true) AS
  SELECT val,n FROM ua UNION ALL SELECT val,n FROM ub;
CREATE MATERIALIZED VIEW un AS
  SELECT val,n FROM ua UNION ALL SELECT val,n FROM ub;
INSERT INTO ua VALUES (6,'y',20),(7,'z',5);
DELETE FROM ub WHERE id=4;
DELETE FROM ua WHERE id=1;
UPDATE ua SET val='z' WHERE id=2;
REFRESH MATERIALIZED VIEW un;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT val,n FROM ui EXCEPT ALL SELECT val,n FROM un)
    UNION ALL (SELECT val,n FROM un EXCEPT ALL SELECT val,n FROM ui)) d;
  IF ndiff=0 THEN RAISE NOTICE 'BUGB UNION ALL multiset (duplicates kept) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUGB: UNION ALL multiset diverged (% rows)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW ui; DROP MATERIALIZED VIEW un; DROP TABLE ua, ub CASCADE;

-- BUG C (audit round 2): a FULL OUTER JOIN row-level matview left a stale
-- NULL-extended phantom when a previously-unmatched row gained a partner (the
-- delete keyed only on the delta-side join column, NULL for that phantom).  Now
-- the affected region is deleted by both sides' keys.  Verify == REFRESH across
-- unmatched->matched and matched->unmatched transitions on both sides.
DROP TABLE IF EXISTS fl, fr CASCADE;
CREATE TABLE fl(k int);
CREATE TABLE fr(k int);
INSERT INTO fr VALUES (4);
CREATE MATERIALIZED VIEW fi WITH (incremental_refresh=true) AS
  SELECT l.k lk, r.k rk FROM fl l FULL OUTER JOIN fr r ON l.k=r.k;
CREATE MATERIALIZED VIEW fn AS
  SELECT l.k lk, r.k rk FROM fl l FULL OUTER JOIN fr r ON l.k=r.k;
INSERT INTO fl VALUES (4);        -- right-only row gains its first left partner
INSERT INTO fl VALUES (8); INSERT INTO fr VALUES (8);   -- mirror
DELETE FROM fl WHERE k=4;         -- matched -> back to right-only
INSERT INTO fr VALUES (9);        -- new right-only
REFRESH MATERIALIZED VIEW fn;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT lk,rk FROM fi EXCEPT ALL SELECT lk,rk FROM fn)
    UNION ALL (SELECT lk,rk FROM fn EXCEPT ALL SELECT lk,rk FROM fi)) d;
  IF ndiff=0 THEN RAISE NOTICE 'BUGC FULL OUTER JOIN (no stale phantom) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUGC: FULL OUTER JOIN diverged (% rows)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW fi; DROP MATERIALIZED VIEW fn; DROP TABLE fl, fr CASCADE;

-- BUG E (audit round 2): a multi-column group key with a partial NULL like
-- (5, NULL) was over-excluded (any-key-NULL), diverging from a REFRESH that
-- keeps it as its own group.  NULL/partial-NULL keys are now maintained with
-- full fidelity (NULLS NOT DISTINCT index + IS NOT DISTINCT FROM joins) for the
-- shared-shell shapes.  (Full coverage in null_key_exclusion.sql.)
DROP TABLE IF EXISTS ek CASCADE;
CREATE TABLE ek(id int primary key, a int, b int, amt numeric NOT NULL);
INSERT INTO ek VALUES (1,1,2,10),(2,5,NULL,7),(3,NULL,NULL,3);
CREATE MATERIALIZED VIEW eki WITH (incremental_refresh=true) AS
  SELECT a,b,SUM(amt) s,COUNT(*) c FROM ek GROUP BY a,b;
CREATE MATERIALIZED VIEW ekn AS
  SELECT a,b,SUM(amt) s,COUNT(*) c FROM ek GROUP BY a,b;
INSERT INTO ek VALUES (4,5,NULL,4),(5,NULL,9,2); DELETE FROM ek WHERE id=3; UPDATE ek SET a=NULL WHERE id=1;
REFRESH MATERIALIZED VIEW ekn;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT a,b,s,c FROM eki EXCEPT SELECT a,b,s,c FROM ekn)
    UNION ALL (SELECT a,b,s,c FROM ekn EXCEPT SELECT a,b,s,c FROM eki)) d;
  IF ndiff=0 THEN RAISE NOTICE 'BUGE multi-key partial-NULL groups maintained == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUGE: partial-NULL key diverged (% rows)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW eki; DROP MATERIALIZED VIEW ekn; DROP TABLE ek CASCADE;

-- BUGF: a MIN/MAX matview's SUM/AVG must stay correct when ONE statement both
-- deletes and inserts on the source (e.g. a data-modifying CTE).  Such a
-- statement fires the INSERT and DELETE statement-triggers separately, so the
-- MIN/MAX delete path must maintain SUM/AVG by delta arithmetic (which composes
-- with the insert delta in any firing order) rather than an absolute rescan
-- (which, combined with the additive insert, would double-count the inserts).
DROP TABLE IF EXISTS cf CASCADE;
CREATE TABLE cf(g int, v int);
INSERT INTO cf VALUES (1,10),(1,20),(1,30),(2,5);
CREATE MATERIALIZED VIEW cfi WITH (incremental_refresh=true) AS
  SELECT g, COUNT(*) c, MIN(v) mn, MAX(v) mx, SUM(v) sm, AVG(v) av FROM cf GROUP BY g;
WITH d AS (DELETE FROM cf WHERE v IN (30,5) RETURNING v),
     i AS (INSERT INTO cf VALUES (1,25),(1,100),(2,8) RETURNING v)
SELECT (SELECT count(*) FROM d) + (SELECT count(*) FROM i);
CREATE MATERIALIZED VIEW cfn AS
  SELECT g, COUNT(*) c, MIN(v) mn, MAX(v) mx, SUM(v) sm, AVG(v) av FROM cf GROUP BY g;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT g,c,mn,mx,sm,av FROM cfi EXCEPT SELECT g,c,mn,mx,sm,av FROM cfn)
    UNION ALL (SELECT g,c,mn,mx,sm,av FROM cfn EXCEPT SELECT g,c,mn,mx,sm,av FROM cfi)) d;
  IF ndiff=0 THEN RAISE NOTICE 'BUGF MIN/MAX SUM/AVG correct under combined DELETE+INSERT == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'BUGF: combined DELETE+INSERT diverged (% rows)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW cfi; DROP MATERIALIZED VIEW cfn; DROP TABLE cf CASCADE;

-- BUGG: MIN/MAX (and any hand-builder shape) over a join of 3+ tables stays
-- correct == REFRESH.  The join-order builder once mis-built the FROM when the
-- delta was a table 2+ join-hops from the leftmost leaf: the leaf (whose ON lives
-- in another entry) was emitted as a bogus CROSS JOIN, dropping the connecting
-- condition and cartesian-joining the delta to the far table (e.g. West pulling
-- in East's MAX).  Now it defers the leaf until its neighbour is known.  This
-- exercises the exact trigger: an UPDATE to the FAR table, plus a group-key
-- rename and fact INSERT/DELETE, over a 3-table chain.
DROP TABLE IF EXISTS gf CASCADE; DROP TABLE IF EXISTS gd CASCADE; DROP TABLE IF EXISTS gr CASCADE;
CREATE TABLE gr(id int primary key, region text, note text);
CREATE TABLE gd(id int primary key, region_id int, cat text);
CREATE TABLE gf(id int primary key, dim_id int, amt int);
INSERT INTO gr VALUES (1,'East','x'),(2,'West','y');
INSERT INTO gd VALUES (10,1,'A'),(20,2,'A');
INSERT INTO gf VALUES (1,10,5),(2,10,50),(3,20,1);
CREATE MATERIALIZED VIEW gg_i WITH (incremental_refresh=true) AS
  SELECT r.region, d.cat, min(f.amt) mn, max(f.amt) mx, count(*) c, sum(f.amt) s
  FROM gf f JOIN gd d ON f.dim_id=d.id JOIN gr r ON d.region_id=r.id
  GROUP BY r.region, d.cat;
UPDATE gr SET note='z' WHERE id=2;        -- far table, irrelevant col: result unchanged
UPDATE gr SET region='East' WHERE id=2;   -- far-table group-key rename (West -> East)
INSERT INTO gf VALUES (4,20,80);          -- new MAX into the merged group
DELETE FROM gf WHERE id=2;                 -- remove the old MAX=50 -> rescan
CREATE MATERIALIZED VIEW gg_n AS
  SELECT r.region, d.cat, min(f.amt) mn, max(f.amt) mx, count(*) c, sum(f.amt) s
  FROM gf f JOIN gd d ON f.dim_id=d.id JOIN gr r ON d.region_id=r.id
  GROUP BY r.region, d.cat;
DO $$
DECLARE ndiff int;
BEGIN
  SELECT count(*) INTO ndiff FROM (
    (SELECT region,cat,mn,mx,c,s FROM gg_i EXCEPT SELECT region,cat,mn,mx,c,s FROM gg_n)
    UNION ALL (SELECT region,cat,mn,mx,c,s FROM gg_n EXCEPT SELECT region,cat,mn,mx,c,s FROM gg_i)) d;
  IF ndiff=0 THEN RAISE NOTICE 'BUGG 3-table MIN/MAX join == REFRESH (far-table UPDATE): PASS';
  ELSE RAISE EXCEPTION 'BUGG: 3-table MIN/MAX join diverged (% rows)', ndiff; END IF;
END $$;
DROP MATERIALIZED VIEW gg_i; DROP MATERIALIZED VIEW gg_n; DROP TABLE gf, gd, gr CASCADE;
\echo ''
\echo '=== audit regression tests complete ==='
