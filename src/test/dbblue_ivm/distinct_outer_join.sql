-- DBblue IVM — recompute aggregates (COUNT(DISTINCT), stddev/variance/bool) over
-- an OUTER join (LEFT / RIGHT).
--
-- Outer-join aggregate matviews are maintained by the Phase 8 recompute builder
-- (incr_build_outer_sql): it recomputes each affected group from the LIVE outer
-- join, preserving orphan rows from the preserved side.  That builder renders
-- the aggregate verbatim — including DISTINCT — so it is correct for the
-- recompute aggregates, not just additive ones.
--
-- SCOPE: enabled for a TWO-TABLE outer join (one preserved + one optional side)
-- whose GROUP BY keys all live on the preserved side — the shape the builder
-- maintains correctly.  3+ table outer-join mixes (an extra INNER-joined
-- dimension), keys on the optional side, FULL OUTER JOIN + GROUP BY, and
-- outer + self-join stay rejected cleanly (see the rejection cases below).
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

-- 7. Out-of-scope outer-join shapes are rejected cleanly (never silently wrong).
DROP TABLE IF EXISTS t3p, t3r, t3c CASCADE;
CREATE TABLE t3p(id int primary key, rid int, g int);
CREATE TABLE t3r(id int primary key, rg text);
CREATE TABLE t3c(id int primary key, pid int, v int);
DO $$
DECLARE made bool;
BEGIN
  -- 3-table INNER+LEFT mix with a recompute aggregate over the optional side
  BEGIN
    made := false;
    CREATE MATERIALIZED VIEW _t3 WITH (incremental_refresh=true) AS
      SELECT p.g, count(DISTINCT c.v) dv FROM t3p p JOIN t3r r ON p.rid=r.id
      LEFT JOIN t3c c ON c.pid=p.id GROUP BY p.g;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN RAISE EXCEPTION '3-table INNER+LEFT DISTINCT: FAIL (accepted)';
  ELSE RAISE NOTICE '3-table INNER+LEFT DISTINCT: PASS (rejected cleanly)'; END IF;

  -- group key on the OPTIONAL (non-preserved) side
  BEGIN
    made := false;
    CREATE MATERIALIZED VIEW _t3 WITH (incremental_refresh=true) AS
      SELECT c.v AS k, count(DISTINCT c.id) dv FROM t3p p LEFT JOIN t3c c ON c.pid=p.id GROUP BY c.v;
    made := true;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN RAISE EXCEPTION 'optional-side group key DISTINCT: FAIL (accepted)';
  ELSE RAISE NOTICE 'optional-side group-key DISTINCT: PASS (rejected cleanly)'; END IF;
END$$;
DROP TABLE t3p, t3r, t3c CASCADE;

\echo ''
\echo '=== DISTINCT / stddev over OUTER join test complete ==='
