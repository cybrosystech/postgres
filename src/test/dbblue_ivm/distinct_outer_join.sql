-- DBblue IVM — recompute aggregates (COUNT(DISTINCT), stddev/variance/bool) over
-- an OUTER join (LEFT / RIGHT).
--
-- Outer-join aggregate matviews are maintained by the Phase 8 recompute builder
-- (incr_build_recompute_sql): it recomputes each affected group from the LIVE outer
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
--
-- FULL OUTER JOIN + GROUP BY is supported for the two-table, single-side
-- plain-column shape (every GROUP BY key is a plain column from ONE of the two
-- joined tables).  There the builder adds a dedicated all-NULL arm for deltas
-- on the key side, covering the other table's orphan (all-NULL) group that the
-- delta-row arm misses (Cases 11-13).
-- A two-table self LEFT/RIGHT join with single-side plain-column GROUP BY keys
-- and additive aggregates is also supported: the same table appears in two
-- roles, so _affected_ is the UNION of both role arms and one combined catalog
-- row is stored.  Preserved-side keys need no orphan arm (Case 14); optional-
-- side keys add an unconditional all-NULL arm for the orphan group (Case 15).
-- Still rejected: FULL + expression/COALESCE key, FULL + mixed-side keys, FULL
-- via USING/NATURAL, 3+-table FULL, FULL self join, self-outer mixed-side keys,
-- DISTINCT/stddev over any self-join, row-level self outer join, and optional-
-- side group keys via multi-hop chains (see Cases 10 and 16).
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
  CREATE TABLE _tfa(id int primary key, k int, j int);
  CREATE TABLE _tfb(id int primary key, k int, j int);
  CREATE TABLE _tfc(id int primary key, k int);

  -- FULL + COALESCE of NON-join columns: an orphan flip CAN relocate a row
  -- between non-NULL groups (a.v ≠ b.v on a match), so this stays rejected.
  -- (COALESCE of the JOIN keys is supported — Case 18.)
  BEGIN
    made := false;
    CREATE MATERIALIZED VIEW _tfmv WITH (incremental_refresh=true) AS
      SELECT COALESCE(a.j,b.j) k, count(*) c
      FROM _tfa a FULL JOIN _tfb b ON a.k=b.k GROUP BY COALESCE(a.j,b.j);
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _tfmv; RAISE EXCEPTION 'FULL+COALESCE(non-key): FAIL (accepted)';
  ELSE RAISE NOTICE 'FULL + COALESCE(non-join cols) still rejected: PASS'; END IF;

  -- FULL + mixed-side keys (one key per table).
  BEGIN
    made := false;
    CREATE MATERIALIZED VIEW _tfmv WITH (incremental_refresh=true) AS
      SELECT a.k ak, b.k bk, count(*) c
      FROM _tfa a FULL JOIN _tfb b ON a.j=b.j GROUP BY a.k, b.k;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _tfmv; RAISE EXCEPTION 'FULL+mixed-side: FAIL (accepted)';
  ELSE RAISE NOTICE 'FULL + mixed-side keys still rejected: PASS'; END IF;

  -- FULL via USING (merged column is COALESCE-like) — must reject cleanly,
  -- not raise an internal error.
  BEGIN
    made := false;
    CREATE MATERIALIZED VIEW _tfmv WITH (incremental_refresh=true) AS
      SELECT k, count(*) c FROM _tfa a FULL JOIN _tfb b USING (k) GROUP BY k;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _tfmv; RAISE EXCEPTION 'FULL USING: FAIL (accepted)';
  ELSE RAISE NOTICE 'FULL via USING still rejected: PASS'; END IF;

  -- 3-table FULL join.
  BEGIN
    made := false;
    CREATE MATERIALIZED VIEW _tfmv WITH (incremental_refresh=true) AS
      SELECT a.k, count(*) c
      FROM _tfa a FULL JOIN _tfb b ON a.k=b.k FULL JOIN _tfc c ON a.k=c.k GROUP BY a.k;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _tfmv; RAISE EXCEPTION '3-table FULL: FAIL (accepted)';
  ELSE RAISE NOTICE 'FULL 3-table still rejected: PASS'; END IF;

  DROP TABLE _tfa, _tfb, _tfc CASCADE;
END$$;

-- 17. M2: DISTINCT over a self LEFT join (preserved-side key) is now SUPPORTED
--     via the self recompute builder — assert acceptance + equality with REFRESH.
DROP TABLE IF EXISTS sd_emp CASCADE;
CREATE TABLE sd_emp(id int primary key, mgr int, v int);
INSERT INTO sd_emp VALUES (1,NULL,10),(2,1,20),(3,1,20),(4,2,30);
CREATE MATERIALIZED VIEW sd_i WITH (incremental_refresh=true) AS
  SELECT e.v gk, count(DISTINCT m.v) dv, count(m.id) cm
  FROM sd_emp e LEFT JOIN sd_emp m ON e.mgr=m.id GROUP BY e.v;
CREATE MATERIALIZED VIEW sd_o AS
  SELECT e.v gk, count(DISTINCT m.v) dv, count(m.id) cm
  FROM sd_emp e LEFT JOIN sd_emp m ON e.mgr=m.id GROUP BY e.v;
INSERT INTO sd_emp VALUES (5,3,40),(6,1,10);
DELETE FROM sd_emp WHERE id=2;
UPDATE sd_emp SET mgr=NULL WHERE id=3;   -- 3 orphaned → NULL manager group
REFRESH MATERIALIZED VIEW sd_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,dv,cm FROM sd_i EXCEPT SELECT gk,dv,cm FROM sd_o) UNION ALL
    (SELECT gk,dv,cm FROM sd_o EXCEPT SELECT gk,dv,cm FROM sd_i)) z;
  IF d=0 THEN RAISE NOTICE 'DISTINCT over self LEFT join (preserved key) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'DISTINCT self-outer: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE sd_emp CASCADE;

-- 18. FULL OUTER JOIN + GROUP BY COALESCE(a.k, b.k) — the key-merge idiom.  The
--     COALESCE of the join keys is invariant under an orphan flip (on a match
--     a.k=b.k), so no NULL group and no relocation; arm 1 of the recompute
--     suffices.  Deltas on both sides, incl. orphan births/deaths.
DROP TABLE IF EXISTS cj_a, cj_b CASCADE;
CREATE TABLE cj_a(id int primary key, k int, v int);
CREATE TABLE cj_b(id int primary key, k int, v int);
INSERT INTO cj_a VALUES (1,1,10),(2,2,20);
INSERT INTO cj_b VALUES (1,2,200),(2,3,300);   -- matched:2 ; a-orphan:1 ; b-orphan:3
CREATE MATERIALIZED VIEW cj_i WITH (incremental_refresh=true) AS
  SELECT COALESCE(a.k,b.k) k, count(*) c, count(a.v) ca, count(b.v) cb, sum(a.v) sa
  FROM cj_a a FULL JOIN cj_b b ON a.k=b.k GROUP BY COALESCE(a.k,b.k);
CREATE MATERIALIZED VIEW cj_o AS
  SELECT COALESCE(a.k,b.k) k, count(*) c, count(a.v) ca, count(b.v) cb, sum(a.v) sa
  FROM cj_a a FULL JOIN cj_b b ON a.k=b.k GROUP BY COALESCE(a.k,b.k);
INSERT INTO cj_a VALUES (3,3,30);   -- de-orphans b(k=3): row stays in group 3 (COALESCE invariant)
DELETE FROM cj_b WHERE k=2;         -- a(k=2) orphans: stays in group 2
INSERT INTO cj_b VALUES (3,9,900);  -- new b-orphan → group 9
UPDATE cj_a SET k=5 WHERE id=1;     -- move group 1 → 5
REFRESH MATERIALIZED VIEW cj_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT k,c,ca,cb,sa FROM cj_i EXCEPT SELECT k,c,ca,cb,sa FROM cj_o) UNION ALL
    (SELECT k,c,ca,cb,sa FROM cj_o EXCEPT SELECT k,c,ca,cb,sa FROM cj_i)) z;
  IF d=0 THEN RAISE NOTICE 'FULL JOIN GROUP BY COALESCE(join keys) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'FULL+COALESCE: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE cj_a, cj_b CASCADE;

-- 11. FULL OUTER JOIN, single-side GROUP BY a.k: NULL-group birth (delete the
--     last match of a b-row → it orphans) and death (insert a match for the
--     lone orphan → NULL group vanishes), plus a delta on the non-key side.
DROP TABLE IF EXISTS fj_a, fj_b CASCADE;
CREATE TABLE fj_a(id int primary key, k int, v int);
CREATE TABLE fj_b(id int primary key, k int, v int);
INSERT INTO fj_a VALUES (1,1,10),(2,2,20);
INSERT INTO fj_b VALUES (1,2,200),(2,3,300);      -- matched:2 ; a-orphan:1 ; b-orphan:3
CREATE MATERIALIZED VIEW fj11_i WITH (incremental_refresh=true) AS
  SELECT a.k gk, count(*) c, count(a.v) ca, count(b.v) cb, sum(b.v) sb
  FROM fj_a a FULL JOIN fj_b b ON a.k=b.k GROUP BY a.k;
CREATE MATERIALIZED VIEW fj11_o AS
  SELECT a.k gk, count(*) c, count(a.v) ca, count(b.v) cb, sum(b.v) sb
  FROM fj_a a FULL JOIN fj_b b ON a.k=b.k GROUP BY a.k;
DELETE FROM fj_a WHERE k=2;         -- b(2) loses its only match → enters NULL group; group 2 vanishes
INSERT INTO fj_a VALUES (3,3,30);   -- matches b(3): b(3) leaves NULL group → group 3 born
INSERT INTO fj_b VALUES (3,9,900);  -- delta on non-key side: new b-orphan → NULL group (arm 1)
DELETE FROM fj_b WHERE k=9;         -- remove that b-orphan again
REFRESH MATERIALIZED VIEW fj11_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,c,ca,cb,sb FROM fj11_i EXCEPT SELECT gk,c,ca,cb,sb FROM fj11_o) UNION ALL
    (SELECT gk,c,ca,cb,sb FROM fj11_o EXCEPT SELECT gk,c,ca,cb,sb FROM fj11_i)) x;
  IF d<>0 THEN RAISE EXCEPTION 'FULL single-side a.k: FAIL (% rows differ)', d;
  ELSE RAISE NOTICE 'FULL JOIN single-side GROUP BY a.k == REFRESH: PASS'; END IF;
END$$;
DROP TABLE fj_a, fj_b CASCADE;

-- 12. FULL OUTER JOIN, single-side GROUP BY b.k (symmetric; delta on b vanishes
--     a group and orphans an a-row into the NULL group).  Reversed table order.
DROP TABLE IF EXISTS fk_a, fk_b CASCADE;
CREATE TABLE fk_a(id int primary key, k int, v int);
CREATE TABLE fk_b(id int primary key, k int, v int);
INSERT INTO fk_a VALUES (1,5,50),(2,6,60);
INSERT INTO fk_b VALUES (1,5,500);                -- matched:5 ; a-orphan:6
CREATE MATERIALIZED VIEW fk12_i WITH (incremental_refresh=true) AS
  SELECT b.k gk, count(*) c, count(a.v) ca, count(b.v) cb
  FROM fk_b b FULL JOIN fk_a a ON a.k=b.k GROUP BY b.k;   -- reversed order, key on 2nd rel
CREATE MATERIALIZED VIEW fk12_o AS
  SELECT b.k gk, count(*) c, count(a.v) ca, count(b.v) cb
  FROM fk_b b FULL JOIN fk_a a ON a.k=b.k GROUP BY b.k;
DELETE FROM fk_b WHERE k=5;         -- a(5) orphans → NULL group (b.k NULL); group 5 vanishes
INSERT INTO fk_b VALUES (2,6,600);  -- matches a(6): a(6) leaves NULL group → group 6 born
REFRESH MATERIALIZED VIEW fk12_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,c,ca,cb FROM fk12_i EXCEPT SELECT gk,c,ca,cb FROM fk12_o) UNION ALL
    (SELECT gk,c,ca,cb FROM fk12_o EXCEPT SELECT gk,c,ca,cb FROM fk12_i)) x;
  IF d<>0 THEN RAISE EXCEPTION 'FULL single-side b.k: FAIL (% rows differ)', d;
  ELSE RAISE NOTICE 'FULL JOIN single-side GROUP BY b.k (reversed order) == REFRESH: PASS'; END IF;
END$$;
DROP TABLE fk_a, fk_b CASCADE;

-- 13. FULL OUTER JOIN with recompute aggregates (COUNT(DISTINCT)/stddev) AND
--     real NULL-key rows in the data sharing the all-NULL group with orphans.
DROP TABLE IF EXISTS fn_a, fn_b CASCADE;
CREATE TABLE fn_a(id int primary key, k int, v int);
CREATE TABLE fn_b(id int primary key, k int, v int);
INSERT INTO fn_a VALUES (1,1,7),(2,1,7),(3,NULL,99);    -- real NULL-key a-row
INSERT INTO fn_b VALUES (1,1,7),(2,1,9),(3,5,500);      -- b(5) orphan → NULL group with a(3)
CREATE MATERIALIZED VIEW fn13_i WITH (incremental_refresh=true) AS
  SELECT a.k gk, count(*) c, count(DISTINCT a.v) da, count(DISTINCT b.v) db,
         stddev_pop(b.v) sdb
  FROM fn_a a FULL JOIN fn_b b ON a.k=b.k GROUP BY a.k;
CREATE MATERIALIZED VIEW fn13_o AS
  SELECT a.k gk, count(*) c, count(DISTINCT a.v) da, count(DISTINCT b.v) db,
         stddev_pop(b.v) sdb
  FROM fn_a a FULL JOIN fn_b b ON a.k=b.k GROUP BY a.k;
INSERT INTO fn_a VALUES (4,NULL,88);  -- another real NULL-key row into the NULL group
DELETE FROM fn_b WHERE k=5;           -- drop the b-orphan from the NULL group
UPDATE fn_a SET k=1 WHERE id=3;       -- move a real NULL-key row into group 1
DELETE FROM fn_a WHERE id=1;          -- shrink group 1
REFRESH MATERIALIZED VIEW fn13_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,c,da,db,sdb FROM fn13_i EXCEPT SELECT gk,c,da,db,sdb FROM fn13_o) UNION ALL
    (SELECT gk,c,da,db,sdb FROM fn13_o EXCEPT SELECT gk,c,da,db,sdb FROM fn13_i)) x;
  IF d<>0 THEN RAISE EXCEPTION 'FULL DISTINCT/stddev + NULL-key data: FAIL (% rows differ)', d;
  ELSE RAISE NOTICE 'FULL JOIN DISTINCT/stddev + real NULL-key rows == REFRESH: PASS'; END IF;
END$$;
DROP TABLE fn_a, fn_b CASCADE;

-- 14. Self OUTER join (same table twice), additive aggregates, GROUP BY on the
--     preserved anchor role.  A delta on the table touches groups in BOTH roles
--     (as employee and as someone's manager); the dual-role _affected_ covers
--     both.  count(DISTINCT)/stddev over a self-join stay rejected (Case 15).
DROP TABLE IF EXISTS so_emp CASCADE;
CREATE TABLE so_emp(id int primary key, mgr int, dept int, sal int);
INSERT INTO so_emp VALUES
  (1,NULL,10,1000),(2,1,10,500),(3,1,20,400),(4,2,20,300),(5,99,30,200);
CREATE MATERIALIZED VIEW so14_i WITH (incremental_refresh=true) AS
  SELECT e.dept gk, count(*) c, count(m.id) cm, sum(m.sal) sms, sum(e.sal) ses
  FROM so_emp e LEFT JOIN so_emp m ON e.mgr = m.id GROUP BY e.dept;
CREATE MATERIALIZED VIEW so14_o AS
  SELECT e.dept gk, count(*) c, count(m.id) cm, sum(m.sal) sms, sum(e.sal) ses
  FROM so_emp e LEFT JOIN so_emp m ON e.mgr = m.id GROUP BY e.dept;
INSERT INTO so_emp VALUES (6,3,20,250);   -- new emp under manager 3
DELETE FROM so_emp WHERE id=1;            -- manager of 2 and 3; also an emp in dept 10
UPDATE so_emp SET mgr=4 WHERE id=5;       -- change a manager link (de-orphan)
UPDATE so_emp SET dept=40 WHERE id=3;     -- move e.dept of a row that is also a manager
DELETE FROM so_emp WHERE id=4;            -- manager of 5 and 6; emp in dept 20
REFRESH MATERIALIZED VIEW so14_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,c,cm,sms,ses FROM so14_i EXCEPT SELECT gk,c,cm,sms,ses FROM so14_o) UNION ALL
    (SELECT gk,c,cm,sms,ses FROM so14_o EXCEPT SELECT gk,c,cm,sms,ses FROM so14_i)) x;
  IF d<>0 THEN RAISE EXCEPTION 'self LEFT join preserved-key: FAIL (% rows differ)', d;
  ELSE RAISE NOTICE 'self LEFT JOIN + GROUP BY preserved key (additive) == REFRESH: PASS'; END IF;
END$$;
DROP TABLE so_emp CASCADE;

-- 15. Self OUTER join, OPTIONAL-side group key: GROUP BY m.dept (the manager's
--     dept).  Employees with no live manager fall in the all-NULL group; a delta
--     on the table flips their orphan status.  The dual-role arms plus the
--     unconditional all-NULL arm cover NULL-group births / deaths / vanishes.
DROP TABLE IF EXISTS so_emp CASCADE;
CREATE TABLE so_emp(id int primary key, mgr int, dept int, sal int);
INSERT INTO so_emp VALUES
  (1,NULL,10,1000),(2,1,10,500),(3,1,20,400),(4,2,20,300),(5,99,30,200);
CREATE MATERIALIZED VIEW so15_i WITH (incremental_refresh=true) AS
  SELECT m.dept gk, count(*) c, count(m.id) cm, sum(e.sal) ses
  FROM so_emp e LEFT JOIN so_emp m ON e.mgr = m.id GROUP BY m.dept;
CREATE MATERIALIZED VIEW so15_o AS
  SELECT m.dept gk, count(*) c, count(m.id) cm, sum(e.sal) ses
  FROM so_emp e LEFT JOIN so_emp m ON e.mgr = m.id GROUP BY m.dept;
DELETE FROM so_emp WHERE id=1;            -- manager of 2,3 → they orphan (NULL grows)
INSERT INTO so_emp VALUES (6,5,40,150);   -- new emp under manager 5 (dept 30)
UPDATE so_emp SET mgr=99 WHERE id=4;      -- 4's manager → missing → 4 orphans (NULL)
UPDATE so_emp SET dept=60 WHERE id=5;     -- manager 5 changes dept: emp 6 moves 30→60
DELETE FROM so_emp WHERE id=5;            -- manager 5 gone → emp 6 orphans; group 60 vanishes
-- partition delete: one statement removes an employee AND its manager → the
-- group they formed (dept 70) appears in neither role arm; the delta⋈delta
-- arm must catch it or a stale row remains
INSERT INTO so_emp VALUES (7,NULL,70,80),(8,7,75,60);  -- manager 7 (dept 70) + emp 8 under it
DELETE FROM so_emp WHERE id IN (7,8);
-- partition UPDATE: one statement changes BOTH partners of a pair (the manager's
-- dept AND the employee's mgr).  LIVE holds post-update values at trigger time,
-- so the old pair exists only in OLD⋈OLD — same delta⋈delta arm, del half of
-- the update
INSERT INTO so_emp VALUES (9,NULL,80,50),(10,9,85,40); -- manager 9 (dept 80) + emp 10 under it
UPDATE so_emp SET dept = CASE WHEN id=9 THEN 90 ELSE dept END,
                  mgr  = CASE WHEN id=10 THEN 99 ELSE mgr END
WHERE id IN (9,10);                                     -- group 80 must vanish
REFRESH MATERIALIZED VIEW so15_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,c,cm,ses FROM so15_i EXCEPT SELECT gk,c,cm,ses FROM so15_o) UNION ALL
    (SELECT gk,c,cm,ses FROM so15_o EXCEPT SELECT gk,c,cm,ses FROM so15_i)) x;
  IF d<>0 THEN RAISE EXCEPTION 'self-outer optional-side key: FAIL (% rows differ)', d;
  ELSE RAISE NOTICE 'self LEFT JOIN optional-side GROUP BY m.dept == REFRESH: PASS'; END IF;
END$$;
DROP TABLE so_emp CASCADE;

-- 16. Self-outer shapes still rejected: FULL self join, mixed-side keys,
--     and row-level (no GROUP BY) self outer join.
DO $$
DECLARE made bool;
BEGIN
  CREATE TABLE so_emp(id int primary key, mgr int, dept int, sal int);

  made := false;
  BEGIN
    CREATE MATERIALIZED VIEW _r WITH (incremental_refresh=true) AS
      SELECT e.dept gk, count(*) c FROM so_emp e FULL JOIN so_emp m ON e.mgr=m.id GROUP BY e.dept;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _r; RAISE EXCEPTION 'FULL self join: FAIL (accepted)';
  ELSE RAISE NOTICE 'FULL self join still rejected: PASS'; END IF;

  made := false;
  BEGIN
    CREATE MATERIALIZED VIEW _r WITH (incremental_refresh=true) AS
      SELECT e.dept ed, m.dept md, count(*) c
      FROM so_emp e LEFT JOIN so_emp m ON e.mgr=m.id GROUP BY e.dept, m.dept;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _r; RAISE EXCEPTION 'self-outer mixed-side keys: FAIL (accepted)';
  ELSE RAISE NOTICE 'self-outer mixed-side keys still rejected: PASS'; END IF;

  made := false;
  BEGIN
    CREATE MATERIALIZED VIEW _r WITH (incremental_refresh=true) AS
      SELECT e.id, m.sal FROM so_emp e LEFT JOIN so_emp m ON e.mgr=m.id;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _r; RAISE EXCEPTION 'row-level self outer: FAIL (accepted)';
  ELSE RAISE NOTICE 'row-level self outer join still rejected: PASS'; END IF;

  DROP TABLE so_emp CASCADE;
END$$;

\echo ''
\echo '=== DISTINCT / stddev over OUTER join test complete ==='
