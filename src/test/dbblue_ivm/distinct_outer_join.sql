-- DBblue IVM — recompute aggregates (COUNT(DISTINCT), stddev/variance/bool) over
-- an OUTER join (LEFT / RIGHT).
--
-- Outer-join aggregate matviews are maintained by the Phase 8 recompute builder
-- (incr_build_outer_sql): it recomputes each affected group from the LIVE outer
-- join, preserving orphan rows from the preserved side.  That builder renders
-- the aggregate verbatim — including DISTINCT — so it is correct for the
-- recompute aggregates, not just additive ones.
--
-- SCOPE: enabled for outer-join matviews with GROUP BY keys on the preserved
-- anchor, INNER-joined dimension tables, OR an optional (LEFT/RIGHT-joined)
-- table that is directly connected to the preserved anchor.  For the last
-- case the builder adds a second UNION arm to _affected_ (orphan detection)
-- that captures preserved rows that changed join status, covering NULL-group
-- births and deaths that the delta-row arm alone cannot see.
-- Still rejected: FULL OUTER JOIN + GROUP BY; outer + self-join; optional-side
-- group keys via multi-hop chains (see Case 10).
--
-- Every case is checked == a full REFRESH of an identically-defined plain
-- matview, including orphan (preserved-only) groups whose DISTINCT count is 0,
-- groups that become/stop being orphans, and preserved-row deletes that vanish a
-- group.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: DISTINCT / stddev over OUTER join ==='
\echo ''

-- 1. LEFT JOIN COUNT(DISTINCT optional-side); orphans -> dv=0, groups that
--    become orphans, distinct churn on the optional side.
DROP TABLE IF EXISTS d_dim, d_fact CASCADE;
CREATE TABLE d_dim(id int primary key, region text);
CREATE TABLE d_fact(id serial primary key, did int, amt int);
INSERT INTO d_dim VALUES (1,'E'),(2,'W'),(3,'N'),(4,'S');   -- S has no facts
INSERT INTO d_fact(did,amt) VALUES (1,10),(1,10),(1,20),(2,5),(3,7);
CREATE MATERIALIZED VIEW lj_i WITH (incremental_refresh=true) AS
  SELECT d.region, count(DISTINCT f.amt) dv, count(f.id) c
  FROM d_dim d LEFT JOIN d_fact f ON f.did=d.id GROUP BY d.region;
CREATE MATERIALIZED VIEW lj_o AS
  SELECT d.region, count(DISTINCT f.amt) dv, count(f.id) c
  FROM d_dim d LEFT JOIN d_fact f ON f.did=d.id GROUP BY d.region;
INSERT INTO d_fact(did,amt) VALUES (4,99),(2,6);   -- orphan S gains a row; W gains a distinct
DELETE FROM d_fact WHERE did=1 AND amt=20;          -- E dv 2->1
DELETE FROM d_fact WHERE did=3;                      -- N becomes orphan (dv->0, c->0)
UPDATE d_fact SET amt=10 WHERE did=2 AND amt=5;      -- W distinct set changes
REFRESH MATERIALIZED VIEW lj_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT region,dv,c FROM lj_i EXCEPT SELECT region,dv,c FROM lj_o)
    UNION ALL (SELECT region,dv,c FROM lj_o EXCEPT SELECT region,dv,c FROM lj_i)) z;
  IF d=0 THEN RAISE NOTICE 'LEFT JOIN COUNT(DISTINCT) incl orphans/dv0/became-orphan == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'LEFT JOIN COUNT(DISTINCT): FAIL (% diff)', d; END IF; END$$;
DROP TABLE d_dim, d_fact CASCADE;

-- 2. preserved-side row delete must vanish the whole group
DROP TABLE IF EXISTS p_dim, p_fact CASCADE;
CREATE TABLE p_dim(id int primary key, region text);
CREATE TABLE p_fact(id serial primary key, did int, amt int);
INSERT INTO p_dim VALUES (1,'E'),(2,'W');
INSERT INTO p_fact(did,amt) VALUES (1,10),(1,20),(2,5);
CREATE MATERIALIZED VIEW pr_i WITH (incremental_refresh=true) AS
  SELECT d.region, count(DISTINCT f.amt) dv FROM p_dim d LEFT JOIN p_fact f ON f.did=d.id GROUP BY d.region;
CREATE MATERIALIZED VIEW pr_o AS
  SELECT d.region, count(DISTINCT f.amt) dv FROM p_dim d LEFT JOIN p_fact f ON f.did=d.id GROUP BY d.region;
DELETE FROM p_dim WHERE id=2;   -- region W vanishes
REFRESH MATERIALIZED VIEW pr_o;
DO $$DECLARE d int; nv int; BEGIN
  SELECT count(*) INTO nv FROM pr_i WHERE region='W';
  SELECT count(*) INTO d FROM ((SELECT region,dv FROM pr_i EXCEPT SELECT region,dv FROM pr_o)
    UNION ALL (SELECT region,dv FROM pr_o EXCEPT SELECT region,dv FROM pr_i)) z;
  IF d=0 AND nv=0 THEN RAISE NOTICE 'LEFT JOIN preserved-row delete vanishes group == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'preserved-row delete: FAIL (diff=%, W rows=%)', d, nv; END IF; END$$;
DROP TABLE p_dim, p_fact CASCADE;

-- 3. stddev over LEFT JOIN (0/1/2+ rows on the optional side)
DROP TABLE IF EXISTS s_dim, s_fact CASCADE;
CREATE TABLE s_dim(id int primary key, k text);
CREATE TABLE s_fact(id serial primary key, did int, v numeric);
INSERT INTO s_dim VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO s_fact(did,v) VALUES (1,10),(1,20),(1,30),(2,5);
CREATE MATERIALIZED VIEW sj_i WITH (incremental_refresh=true) AS
  SELECT d.k, stddev(f.v) sv, count(f.id) c FROM s_dim d LEFT JOIN s_fact f ON f.did=d.id GROUP BY d.k;
CREATE MATERIALIZED VIEW sj_o AS
  SELECT d.k, stddev(f.v) sv, count(f.id) c FROM s_dim d LEFT JOIN s_fact f ON f.did=d.id GROUP BY d.k;
INSERT INTO s_fact(did,v) VALUES (2,500),(3,1);   -- b -> 2 rows, c -> 1 row
DELETE FROM s_fact WHERE did=1 AND v=30;
REFRESH MATERIALIZED VIEW sj_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT k,sv,c FROM sj_i EXCEPT SELECT k,sv,c FROM sj_o)
    UNION ALL (SELECT k,sv,c FROM sj_o EXCEPT SELECT k,sv,c FROM sj_i)) z;
  IF d=0 THEN RAISE NOTICE 'LEFT JOIN stddev (incl NULL / 1-row groups) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'LEFT JOIN stddev: FAIL (% diff)', d; END IF; END$$;
DROP TABLE s_dim, s_fact CASCADE;

-- 4. RIGHT JOIN COUNT(DISTINCT)
DROP TABLE IF EXISTS r_a, r_b CASCADE;
CREATE TABLE r_a(id serial primary key, bid int, amt int);
CREATE TABLE r_b(id int primary key, grp text);
INSERT INTO r_b VALUES (1,'x'),(2,'y'),(3,'z');
INSERT INTO r_a(bid,amt) VALUES (1,10),(1,10),(2,5);
CREATE MATERIALIZED VIEW rj_i WITH (incremental_refresh=true) AS
  SELECT b.grp, count(DISTINCT a.amt) dv FROM r_a a RIGHT JOIN r_b b ON a.bid=b.id GROUP BY b.grp;
CREATE MATERIALIZED VIEW rj_o AS
  SELECT b.grp, count(DISTINCT a.amt) dv FROM r_a a RIGHT JOIN r_b b ON a.bid=b.id GROUP BY b.grp;
INSERT INTO r_a(bid,amt) VALUES (3,7),(1,20);
DELETE FROM r_a WHERE bid=2;   -- y becomes orphan (dv->0)
REFRESH MATERIALIZED VIEW rj_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT grp,dv FROM rj_i EXCEPT SELECT grp,dv FROM rj_o)
    UNION ALL (SELECT grp,dv FROM rj_o EXCEPT SELECT grp,dv FROM rj_i)) z;
  IF d=0 THEN RAISE NOTICE 'RIGHT JOIN COUNT(DISTINCT) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'RIGHT JOIN COUNT(DISTINCT): FAIL (% diff)', d; END IF; END$$;
DROP TABLE r_a, r_b CASCADE;

-- 5. LEFT JOIN COUNT(DISTINCT) + HAVING (boundary crossings, incl. WITH-DATA fails)
DROP TABLE IF EXISTS h_dim, h_fact CASCADE;
CREATE TABLE h_dim(id int primary key, region text);
CREATE TABLE h_fact(id serial primary key, did int, amt int);
INSERT INTO h_dim VALUES (1,'E'),(2,'W'),(3,'N');
INSERT INTO h_fact(did,amt) VALUES (1,10),(2,5),(3,7),(3,7);   -- all dv<=1 -> all FAIL HAVING>=2
CREATE MATERIALIZED VIEW hj_i WITH (incremental_refresh=true) AS
  SELECT d.region, count(DISTINCT f.amt) dv FROM h_dim d LEFT JOIN h_fact f ON f.did=d.id
  GROUP BY d.region HAVING count(DISTINCT f.amt) >= 2;
INSERT INTO h_fact(did,amt) VALUES (1,11),(2,6);   -- E,W cross up
CREATE MATERIALIZED VIEW hj_o AS
  SELECT d.region, count(DISTINCT f.amt) dv FROM h_dim d LEFT JOIN h_fact f ON f.did=d.id
  GROUP BY d.region HAVING count(DISTINCT f.amt) >= 2;
DELETE FROM h_fact WHERE did=1 AND amt=11;          -- E crosses back down
REFRESH MATERIALIZED VIEW hj_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT region,dv FROM hj_i EXCEPT SELECT region,dv FROM hj_o)
    UNION ALL (SELECT region,dv FROM hj_o EXCEPT SELECT region,dv FROM hj_i)) z;
  IF d=0 THEN RAISE NOTICE 'LEFT JOIN COUNT(DISTINCT)+HAVING boundary (WITH DATA) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'LEFT JOIN COUNT(DISTINCT)+HAVING: FAIL (% diff)', d; END IF; END$$;
DROP TABLE h_dim, h_fact CASCADE;

-- 6. NULL preserved-side group key: orphan group with a NULL key must vanish on
--    delete of its last preserved row and appear when a key is updated to NULL
--    (regression: the builder now matches group keys with IS NOT DISTINCT FROM).
DROP TABLE IF EXISTS nd, nf CASCADE;
CREATE TABLE nd(did int, region text);   -- nullable group key, no PK
CREATE TABLE nf(id serial primary key, fdid int, sal int);
INSERT INTO nd VALUES (1,'A'),(NULL,'N'),(2,'B');
INSERT INTO nf(fdid,sal) VALUES (1,100),(1,200);
CREATE MATERIALIZED VIEW n_i WITH (incremental_refresh=true) AS
  SELECT d.did, count(DISTINCT f.sal) dv, stddev(f.sal) sd
  FROM nd d LEFT JOIN nf f ON f.fdid=d.did GROUP BY d.did;
CREATE MATERIALIZED VIEW n_o AS
  SELECT d.did, count(DISTINCT f.sal) dv, stddev(f.sal) sd
  FROM nd d LEFT JOIN nf f ON f.fdid=d.did GROUP BY d.did;
DELETE FROM nd WHERE did IS NULL;       -- existing NULL group must vanish
UPDATE nd SET did=NULL WHERE region='B'; -- a new NULL orphan group must appear
REFRESH MATERIALIZED VIEW n_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT did,dv,sd FROM n_i EXCEPT SELECT did,dv,sd FROM n_o)
    UNION ALL (SELECT did,dv,sd FROM n_o EXCEPT SELECT did,dv,sd FROM n_i)) z;
  IF d=0 THEN RAISE NOTICE 'LEFT JOIN NULL preserved-key (delete + update-to-NULL) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'NULL preserved-key: FAIL (% diff)', d; END IF; END$$;
DROP TABLE nd, nf CASCADE;

-- 7. 3-table INNER+LEFT: COUNT(DISTINCT) on optional side, GROUP BY preserved key.
--    Deleting from the INNER-joined dimension (t3r) must vanish the affected groups;
--    deleting from the optional side (t3c) updates the distinct count.
DROP TABLE IF EXISTS t3p, t3r, t3c CASCADE;
CREATE TABLE t3p(id int primary key, rid int, g int);
CREATE TABLE t3r(id int primary key, rg text);
CREATE TABLE t3c(id int primary key, pid int, v int);
INSERT INTO t3r VALUES (1,'A'),(2,'B'),(3,'C');
INSERT INTO t3p VALUES (1,1,10),(2,1,10),(3,2,20),(4,3,30);
INSERT INTO t3c VALUES (1,1,5),(2,1,5),(3,1,7),(4,2,9),(5,2,9);
CREATE MATERIALIZED VIEW t3_i WITH (incremental_refresh=true) AS
  SELECT p.g, count(DISTINCT c.v) dv FROM t3p p JOIN t3r r ON p.rid=r.id
  LEFT JOIN t3c c ON c.pid=p.id GROUP BY p.g;
CREATE MATERIALIZED VIEW t3_o AS
  SELECT p.g, count(DISTINCT c.v) dv FROM t3p p JOIN t3r r ON p.rid=r.id
  LEFT JOIN t3c c ON c.pid=p.id GROUP BY p.g;
-- optional-side changes
INSERT INTO t3c VALUES (6,1,99),(7,3,1);
DELETE FROM t3c WHERE v=9;
-- inner-dim delete: delete r row 3 → p row 4 (g=30) loses its r match → group must vanish
DELETE FROM t3r WHERE id=3;
REFRESH MATERIALIZED VIEW t3_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT g,dv FROM t3_i EXCEPT SELECT g,dv FROM t3_o)
    UNION ALL (SELECT g,dv FROM t3_o EXCEPT SELECT g,dv FROM t3_i)) z;
  IF d=0 THEN RAISE NOTICE '3-table INNER+LEFT COUNT(DISTINCT) == REFRESH: PASS';
  ELSE RAISE EXCEPTION '3-table INNER+LEFT COUNT(DISTINCT): FAIL (% diff)', d; END IF;
END$$;
DROP TABLE t3p, t3r, t3c CASCADE;

-- 8. 3-table INNER+LEFT: stddev on optional side + GROUP BY inner-dim key.
DROP TABLE IF EXISTS s3p, s3r, s3f CASCADE;
CREATE TABLE s3p(id int primary key, rid int);
CREATE TABLE s3r(id int primary key, lbl text);
CREATE TABLE s3f(id serial primary key, pid int, val numeric);
INSERT INTO s3r VALUES (1,'X'),(2,'Y'),(3,'Z');
INSERT INTO s3p VALUES (1,1),(2,1),(3,2),(4,3);
INSERT INTO s3f(pid,val) VALUES (1,10),(1,20),(1,30),(3,5),(3,5);
CREATE MATERIALIZED VIEW s3_i WITH (incremental_refresh=true) AS
  SELECT r.lbl, stddev(f.val) sv, count(f.id) c
  FROM s3p p JOIN s3r r ON p.rid=r.id LEFT JOIN s3f f ON f.pid=p.id GROUP BY r.lbl;
CREATE MATERIALIZED VIEW s3_o AS
  SELECT r.lbl, stddev(f.val) sv, count(f.id) c
  FROM s3p p JOIN s3r r ON p.rid=r.id LEFT JOIN s3f f ON f.pid=p.id GROUP BY r.lbl;
INSERT INTO s3f(pid,val) VALUES (2,100),(4,50);
DELETE FROM s3f WHERE pid=1 AND val=30;
-- delete inner-dim r row 3 → p row 4 loses its r match → group Z must vanish
DELETE FROM s3r WHERE id=3;
REFRESH MATERIALIZED VIEW s3_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT lbl,sv,c FROM s3_i EXCEPT SELECT lbl,sv,c FROM s3_o)
    UNION ALL (SELECT lbl,sv,c FROM s3_o EXCEPT SELECT lbl,sv,c FROM s3_i)) z;
  IF d=0 THEN RAISE NOTICE '3-table INNER+LEFT stddev (GROUP BY inner key) == REFRESH: PASS';
  ELSE RAISE EXCEPTION '3-table INNER+LEFT stddev: FAIL (% diff)', d; END IF;
END$$;
DROP TABLE s3p, s3r, s3f CASCADE;

-- 9. Optional-side group key: GROUP BY c.k where c is LEFT-joined.
--    Delete from c can orphan preserved rows → they join the NULL group.
--    Insert into c can de-orphan preserved rows → NULL group shrinks/vanishes.
--    The dual-arm _affected_ CTE handles both correctly.
DROP TABLE IF EXISTS ok_p, ok_c CASCADE;
CREATE TABLE ok_p(id int primary key, label text);
CREATE TABLE ok_c(id int primary key, pid int, k int, v int);
INSERT INTO ok_p VALUES (1,'a'),(2,'b'),(3,'c');
-- p.id=3 is initially orphaned (no c match) → NULL group exists from the start
INSERT INTO ok_c VALUES (1,1,10,100),(2,1,10,200),(3,2,20,300);

CREATE MATERIALIZED VIEW ok9_i WITH (incremental_refresh=true) AS
  SELECT c.k, count(DISTINCT c.v) dv, count(c.id) cnt
  FROM ok_p p LEFT JOIN ok_c c ON c.pid=p.id
  GROUP BY c.k;
CREATE MATERIALIZED VIEW ok9_o AS
  SELECT c.k, count(DISTINCT c.v) dv, count(c.id) cnt
  FROM ok_p p LEFT JOIN ok_c c ON c.pid=p.id
  GROUP BY c.k;

-- Delete all c rows for p.id=2 (k=20): p.id=2 becomes orphaned → NULL group grows
DELETE FROM ok_c WHERE pid=2;
-- Insert a new c row for previously-orphaned p.id=3 → NULL group shrinks; k=30 appears
INSERT INTO ok_c VALUES (4,3,30,400);

REFRESH MATERIALIZED VIEW ok9_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT k,dv,cnt FROM ok9_i EXCEPT SELECT k,dv,cnt FROM ok9_o)
    UNION ALL
    (SELECT k,dv,cnt FROM ok9_o EXCEPT SELECT k,dv,cnt FROM ok9_i)
  ) z;
  IF d=0 THEN RAISE NOTICE 'optional-side GROUP BY key (del→orphan, ins→de-orphan) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'optional-side GROUP BY key: FAIL (% diffs)', d; END IF;
END$$;
DROP TABLE ok_p, ok_c CASCADE;

-- 9b. Optional-side group key with additive COUNT(*) (no DISTINCT — a
--     different code path, but arm 2 fixes it for the same reason).
DROP TABLE IF EXISTS ok2_p, ok2_c CASCADE;
CREATE TABLE ok2_p(id int primary key);
CREATE TABLE ok2_c(id int primary key, pid int, k text);
INSERT INTO ok2_p VALUES (1),(2),(3);
-- p.id=3 initially orphaned
INSERT INTO ok2_c VALUES (1,1,'A'),(2,1,'A'),(3,2,'B');

CREATE MATERIALIZED VIEW ok9b_i WITH (incremental_refresh=true) AS
  SELECT c.k, count(*) cnt
  FROM ok2_p p LEFT JOIN ok2_c c ON c.pid=p.id
  GROUP BY c.k;
CREATE MATERIALIZED VIEW ok9b_o AS
  SELECT c.k, count(*) cnt
  FROM ok2_p p LEFT JOIN ok2_c c ON c.pid=p.id
  GROUP BY c.k;

-- Delete B: p.id=2 becomes orphaned → NULL group grows from 1 to 2
DELETE FROM ok2_c WHERE k='B';
-- Insert C for p.id=3 (was orphaned): NULL group shrinks from 2 to 1; C appears
INSERT INTO ok2_c VALUES (4,3,'C');

REFRESH MATERIALIZED VIEW ok9b_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT k,cnt FROM ok9b_i EXCEPT SELECT k,cnt FROM ok9b_o)
    UNION ALL
    (SELECT k,cnt FROM ok9b_o EXCEPT SELECT k,cnt FROM ok9b_i)
  ) z;
  IF d=0 THEN RAISE NOTICE 'optional-side GROUP BY key + COUNT(*) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'optional-side GROUP BY key + COUNT(*): FAIL (% diffs)', d; END IF;
END$$;
DROP TABLE ok2_p, ok2_c CASCADE;

-- 10. Remaining out-of-scope shapes still rejected cleanly.
DO $$
DECLARE made bool;
BEGIN
  -- FULL OUTER JOIN + GROUP BY: both sides can orphan rows into NULL groups;
  -- dual-arm cannot be built simply for both directions simultaneously.
  BEGIN
    made := false;
    CREATE TABLE _tfa(id int primary key, k int);
    CREATE TABLE _tfb(id int primary key, k int);
    CREATE MATERIALIZED VIEW _tfmv WITH (incremental_refresh=true) AS
      SELECT a.k FROM _tfa a FULL JOIN _tfb b ON a.k=b.k GROUP BY a.k;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  DROP TABLE IF EXISTS _tfa, _tfb CASCADE;
  IF made THEN RAISE EXCEPTION 'FULL JOIN + GROUP BY: FAIL (accepted)';
  ELSE RAISE NOTICE 'FULL JOIN + GROUP BY still rejected: PASS'; END IF;

  -- Outer join + self-join: dedicated self-join path; not recompute-outer shape.
  BEGIN
    made := false;
    CREATE TABLE _tsa(id int primary key, pid int, v int);
    CREATE MATERIALIZED VIEW _tsamv WITH (incremental_refresh=true) AS
      SELECT a.v, count(DISTINCT b.v) dv
      FROM _tsa a LEFT JOIN _tsa b ON a.pid=b.id GROUP BY a.v;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  DROP TABLE IF EXISTS _tsa CASCADE;
  IF made THEN RAISE EXCEPTION 'outer join + self-join: FAIL (accepted)';
  ELSE RAISE NOTICE 'outer join + self-join still rejected: PASS'; END IF;
END$$;

\echo ''
\echo '=== DISTINCT / stddev over OUTER join test complete ==='
