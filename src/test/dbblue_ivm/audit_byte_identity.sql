-- audit_byte_identity.sql
--
-- Regression guards for four byte-identity defects found by the adversarial
-- byte-identity audit (each was: incremental != full REFRESH on a shape plain
-- Postgres accepts).  All four are now fixed; this file locks them down.
--
--   1. MIN/MAX over a join whose group-key name also exists in the other table:
--      the old_delta CTE emitted a bare "GROUP BY <name>" -> ambiguous -> every
--      delta errored (accept-then-fail).
--   2. Row-level self-join with a NULL projected column: the DELETE matched with
--      "(cols) IN (subquery)" (= semantics) so a NULL column never matched and
--      the row was left stale on delete.
--   3. SUM/AVG over an UNCONSTRAINED numeric: the additive running total keeps
--      the max dscale ever accumulated, so removing a higher-scale contributor
--      left stale trailing zeros (numerically equal, byte-different).  Fixed by
--      recomputing only the unconstrained case; fixed-scale numeric(p,s) stays
--      on the additive fast path.
--   4. Multi-table additive aggregate must serialize on the matview lock (a
--      dimension-side group-key change reads the other table through the join,
--      which is not serializable unlocked).  Verified structurally here; the
--      concurrent stress lives in the shell harness.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: byte-identity audit regressions ==='

-- ---------- 1. MIN/MAX over join, colliding group-key name ----------
DROP TABLE IF EXISTS ab_dim, ab_fact CASCADE;
CREATE TABLE ab_dim(did int primary key, dname text);
CREATE TABLE ab_fact(fid int primary key, did int, amt numeric);
INSERT INTO ab_dim VALUES (1,'a'),(2,'b'),(3,'c');
INSERT INTO ab_fact VALUES (10,1,100),(11,1,200),(12,2,50);
CREATE MATERIALIZED VIEW ab1_m WITH (incremental_refresh=true) AS
  SELECT d.did, min(f.amt) mn, max(f.amt) mx, count(*) c FROM ab_dim d JOIN ab_fact f ON f.did=d.did GROUP BY d.did;
CREATE MATERIALIZED VIEW ab1_r AS
  SELECT d.did, min(f.amt) mn, max(f.amt) mx, count(*) c FROM ab_dim d JOIN ab_fact f ON f.did=d.did GROUP BY d.did;
DO $$
DECLARE d int;
BEGIN
  INSERT INTO ab_fact VALUES (15,1,300);      -- these all errored before ("did is ambiguous")
  DELETE FROM ab_fact WHERE fid=11;
  UPDATE ab_fact SET amt=amt+1 WHERE fid=12;
  DELETE FROM ab_dim WHERE did=3;
  REFRESH MATERIALIZED VIEW ab1_r;
  SELECT count(*) INTO d FROM (
    (SELECT did,mn,mx,c FROM ab1_m EXCEPT SELECT did,mn,mx,c FROM ab1_r)
    UNION ALL (SELECT did,mn,mx,c FROM ab1_r EXCEPT SELECT did,mn,mx,c FROM ab1_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'min/max-over-join colliding key: diverged by %', d; END IF;
  RAISE NOTICE '1. min/max over join with colliding group-key name == REFRESH: PASS';
END $$;
DROP MATERIALIZED VIEW ab1_m, ab1_r CASCADE; DROP TABLE ab_dim, ab_fact CASCADE;

-- ---------- 2. Row-level self-join, NULL projected column ----------
DROP TABLE IF EXISTS ab_c, ab_f CASCADE;
CREATE TABLE ab_c(cid int primary key, region text);
CREATE TABLE ab_f(fid int primary key, parent int, dkey int, qty numeric);
INSERT INTO ab_c VALUES (1, NULL), (2, 'north');
INSERT INTO ab_f VALUES (10,NULL,1,100),(20,10,1,200),(30,NULL,2,300),(40,30,2,400);
CREATE MATERIALIZED VIEW ab2_m WITH (incremental_refresh=true) AS
  SELECT f.fid, f.qty cq, p.qty pq, p.fid pid, c.region FROM ab_f f JOIN ab_f p ON f.parent=p.fid JOIN ab_c c ON f.dkey=c.cid;
CREATE MATERIALIZED VIEW ab2_r AS
  SELECT f.fid, f.qty cq, p.qty pq, p.fid pid, c.region FROM ab_f f JOIN ab_f p ON f.parent=p.fid JOIN ab_c c ON f.dkey=c.cid;
DO $$
DECLARE d int;
BEGIN
  DELETE FROM ab_f WHERE fid=10;              -- deletes the NULL-region row's partner (was: left stale)
  UPDATE ab_f SET qty=qty+1 WHERE fid=40;
  INSERT INTO ab_f VALUES (50, 30, 1, 500);
  REFRESH MATERIALIZED VIEW ab2_r;
  SELECT count(*) INTO d FROM (
    (SELECT fid,cq,pq,pid,region FROM ab2_m EXCEPT SELECT fid,cq,pq,pid,region FROM ab2_r)
    UNION ALL (SELECT fid,cq,pq,pid,region FROM ab2_r EXCEPT SELECT fid,cq,pq,pid,region FROM ab2_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'row-level self-join NULL column: diverged by %', d; END IF;
  RAISE NOTICE '2. row-level self-join with NULL projected column == REFRESH: PASS';
END $$;
DROP MATERIALIZED VIEW ab2_m, ab2_r CASCADE; DROP TABLE ab_c, ab_f CASCADE;

-- ---------- 3. numeric SUM/AVG dscale: unconstrained byte-identical, fixed-scale additive ----------
DROP TABLE IF EXISTS ab_ns, ab_fs CASCADE;
CREATE TABLE ab_ns(id int primary key, g text, v numeric);        -- unconstrained
CREATE TABLE ab_fs(id int primary key, g text, v numeric(16,2));  -- fixed scale
INSERT INTO ab_ns VALUES (1,'a',1.00),(2,'a',2.00000),(3,'b',5.0),(4,'b',3.000);
INSERT INTO ab_fs VALUES (1,'a',1.00),(2,'a',2.00),(3,'b',5.00);
CREATE MATERIALIZED VIEW ab3n_m WITH (incremental_refresh=true) AS SELECT g, sum(v) s, avg(v) av FROM ab_ns GROUP BY g;
CREATE MATERIALIZED VIEW ab3n_r AS SELECT g, sum(v) s, avg(v) av FROM ab_ns GROUP BY g;
CREATE MATERIALIZED VIEW ab3f_m WITH (incremental_refresh=true) AS SELECT g, sum(v) s FROM ab_fs GROUP BY g;
CREATE MATERIALIZED VIEW ab3f_r AS SELECT g, sum(v) s FROM ab_fs GROUP BY g;
DO $$
DECLARE d int; ndel text;
BEGIN
  DELETE FROM ab_ns WHERE id=2;               -- remove higher-scale contributor
  UPDATE ab_ns SET v=4.0 WHERE id=4;
  INSERT INTO ab_ns VALUES (5,'a',7.000000);
  REFRESH MATERIALIZED VIEW ab3n_r;
  -- byte-identity via ::text
  SELECT count(*) INTO d FROM (
    (SELECT g,s::text,av::text FROM ab3n_m EXCEPT SELECT g,s::text,av::text FROM ab3n_r)
    UNION ALL (SELECT g,s::text,av::text FROM ab3n_r EXCEPT SELECT g,s::text,av::text FROM ab3n_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'unconstrained numeric sum/avg: byte-diverged by %', d; END IF;
  -- unconstrained must be on the recompute path
  SELECT del_sql INTO ndel FROM pg_dbblue_matview WHERE mvrelid='ab3n_m'::regclass LIMIT 1;
  IF ndel NOT LIKE '%_affected_%' THEN RAISE EXCEPTION 'unconstrained numeric sum should recompute'; END IF;

  DELETE FROM ab_fs WHERE id=2; INSERT INTO ab_fs VALUES (4,'b',9.00);
  REFRESH MATERIALIZED VIEW ab3f_r;
  SELECT count(*) INTO d FROM (
    (SELECT g,s::text FROM ab3f_m EXCEPT SELECT g,s::text FROM ab3f_r)
    UNION ALL (SELECT g,s::text FROM ab3f_r EXCEPT SELECT g,s::text FROM ab3f_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'fixed-scale numeric sum: diverged by %', d; END IF;
  -- fixed-scale must stay on the additive (no _affected_) path
  SELECT del_sql INTO ndel FROM pg_dbblue_matview WHERE mvrelid='ab3f_m'::regclass LIMIT 1;
  IF ndel LIKE '%_affected_%' THEN RAISE EXCEPTION 'fixed-scale numeric sum should stay additive'; END IF;
  RAISE NOTICE '3. numeric sum/avg: unconstrained byte-identical (recompute), fixed-scale additive: PASS';
END $$;
DROP MATERIALIZED VIEW ab3n_m, ab3n_r, ab3f_m, ab3f_r CASCADE; DROP TABLE ab_ns, ab_fs CASCADE;

-- ---------- 4. multi-table additive aggregate serializes on the matview lock ----------
DROP TABLE IF EXISTS ab_h, ab_l CASCADE;
CREATE TABLE ab_h(id int primary key, region text);
CREATE TABLE ab_l(id int primary key, head_id int, amt numeric);
INSERT INTO ab_h SELECT g,'r'||(g%3) FROM generate_series(1,6) g;
INSERT INTO ab_l SELECT g,(g%6)+1,g FROM generate_series(1,60) g;
CREATE MATERIALIZED VIEW ab4_m WITH (incremental_refresh=true) AS
  SELECT h.region, count(*) c, sum(l.amt) s FROM ab_h h JOIN ab_l l ON l.head_id=h.id GROUP BY h.region;
DO $$
DECLARE nlock int;
BEGIN
  -- every source table's catalog row must carry a lock_sql (advisory lock) so
  -- concurrent dimension-key changes + fact deltas serialize (else silent drift)
  SELECT count(*) INTO nlock FROM pg_dbblue_matview
   WHERE mvrelid='ab4_m'::regclass AND (lock_sql IS NULL OR lock_sql = '');
  IF nlock <> 0 THEN
    RAISE EXCEPTION 'multi-table additive aggregate has % unlocked delta path(s)', nlock; END IF;
  RAISE NOTICE '4. multi-table additive aggregate serializes on the matview lock: PASS';
END $$;
DROP MATERIALIZED VIEW ab4_m CASCADE; DROP TABLE ab_h, ab_l CASCADE;
\echo 'PASS: byte-identity audit regressions all green'
