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

-- ---------- 2. Row-level join, NULL projected column (NULL-safe delete match) ----------
-- The row-level DELETE matches matview rows by all projected columns; a plain
-- "= " match makes a NULL column UNKNOWN and never matches, leaving the row
-- stale.  The count-based delete matches with IS NOT DISTINCT FROM.
DROP TABLE IF EXISTS ab_c, ab_f CASCADE;
CREATE TABLE ab_c(cid int primary key, region text);
CREATE TABLE ab_f(fid int primary key, dkey int, qty numeric);
INSERT INTO ab_c VALUES (1, NULL), (2, 'north');      -- region 1 is NULL
INSERT INTO ab_f VALUES (10,1,100),(20,1,200),(30,2,300),(40,2,400);
CREATE MATERIALIZED VIEW ab2_m WITH (incremental_refresh=true) AS
  SELECT f.fid, f.qty cq, c.region FROM ab_f f JOIN ab_c c ON f.dkey=c.cid;
CREATE MATERIALIZED VIEW ab2_r AS
  SELECT f.fid, f.qty cq, c.region FROM ab_f f JOIN ab_c c ON f.dkey=c.cid;
DO $$
DECLARE d int;
BEGIN
  DELETE FROM ab_f WHERE fid=10;              -- a NULL-region row (was: left stale)
  UPDATE ab_f SET qty=qty+1 WHERE fid=40;
  INSERT INTO ab_f VALUES (50, 1, 500);       -- another NULL-region row
  REFRESH MATERIALIZED VIEW ab2_r;
  SELECT count(*) INTO d FROM (
    (SELECT fid,cq,region FROM ab2_m EXCEPT SELECT fid,cq,region FROM ab2_r)
    UNION ALL (SELECT fid,cq,region FROM ab2_r EXCEPT SELECT fid,cq,region FROM ab2_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'row-level NULL column: diverged by %', d; END IF;
  RAISE NOTICE '2. row-level join with NULL projected column == REFRESH: PASS';
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

-- ---------- 5. self-joins: row-level rejected; aggregate maintains under join-key churn ----------
-- A row-level (non-aggregated) self-join is REJECTED: both role arms re-derive
-- the self-matching diagonal pair (for ON a.k=b.k, every row), which an INSERT
-- double-counts and a DELETE cannot reconcile.  An AGGREGATE self-join uses the
-- recompute path and stays byte-identical even under join-key UPDATEs.
DROP TABLE IF EXISTS ab_sj CASCADE;
CREATE TABLE ab_sj(id int primary key, parent int, k int, cat text, val int);
INSERT INTO ab_sj VALUES (6,NULL,0,'a',60),(13,6,1,'a',130),(26,13,1,'b',260),(27,13,2,'b',270),(52,26,2,'a',520);
CREATE MATERIALIZED VIEW ab5agg_m WITH (incremental_refresh=true) AS
  SELECT a.cat, count(*) c, sum(b.val) s FROM ab_sj a JOIN ab_sj b ON a.k=b.k GROUP BY a.cat;
CREATE MATERIALIZED VIEW ab5agg_r AS
  SELECT a.cat, count(*) c, sum(b.val) s FROM ab_sj a JOIN ab_sj b ON a.k=b.k GROUP BY a.cat;
DO $$
DECLARE d int;
BEGIN
  -- row-level self-join must be rejected
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ab5row WITH (incremental_refresh=true) AS
             SELECT a.id, b.val FROM ab_sj a JOIN ab_sj b ON a.k=b.k';
    RAISE EXCEPTION 'row-level self-join should be rejected';
  EXCEPTION WHEN feature_not_supported THEN NULL; END;

  -- aggregate self-join stays byte-identical under join-key + measure + membership churn
  UPDATE ab_sj SET k=k+1 WHERE id=13;             -- join-key change on a both-sides node
  INSERT INTO ab_sj VALUES (99,6,1,'a',990);
  UPDATE ab_sj SET val=val+1 WHERE id=6;
  DELETE FROM ab_sj WHERE id=27;
  REFRESH MATERIALIZED VIEW ab5agg_r;
  SELECT count(*) INTO d FROM (
    (SELECT cat,c,s FROM ab5agg_m EXCEPT ALL SELECT cat,c,s FROM ab5agg_r)
    UNION ALL (SELECT cat,c,s FROM ab5agg_r EXCEPT ALL SELECT cat,c,s FROM ab5agg_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'aggregate self-join join-key churn: diverged by %', d; END IF;
  RAISE NOTICE '5. row-level self-join rejected; aggregate self-join join-key churn == REFRESH: PASS';
END $$;
DROP MATERIALIZED VIEW ab5agg_m, ab5agg_r CASCADE; DROP TABLE ab_sj CASCADE;

-- ---------- 6. unsupported shapes are cleanly REJECTED (not accepted-then-fail/diverge) ----------
DROP TABLE IF EXISTS ab_part, ab_gs CASCADE;
CREATE TABLE ab_part(id int, dt date, grp int, amt numeric, PRIMARY KEY(id,dt)) PARTITION BY RANGE(dt);
CREATE TABLE ab_part_a PARTITION OF ab_part FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
INSERT INTO ab_part SELECT g,'2024-06-01'::date, g%3, g FROM generate_series(1,20) g;
CREATE TABLE ab_gs(id int primary key, g text, f text, v numeric);
INSERT INTO ab_gs SELECT g,'g'||(g%3),'f'||(g%2),g FROM generate_series(1,20) g;
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ab6_bad1 WITH (incremental_refresh=true) AS SELECT grp,count(*) c FROM ab_part GROUP BY grp';
    RAISE EXCEPTION 'partitioned source should be rejected';
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ab6_bad2 WITH (incremental_refresh=true) AS SELECT g,f,count(*) c FROM ab_gs GROUP BY ROLLUP(g,f)';
    RAISE EXCEPTION 'ROLLUP should be rejected';
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ab6_bad3 WITH (incremental_refresh=true) AS SELECT g,f,count(*) c FROM ab_gs GROUP BY GROUPING SETS ((g),(f))';
    RAISE EXCEPTION 'GROUPING SETS should be rejected';
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ab6_bad4 WITH (incremental_refresh=true) AS SELECT g,count(*) c FROM ab_gs TABLESAMPLE BERNOULLI(50) GROUP BY g';
    RAISE EXCEPTION 'TABLESAMPLE should be rejected';
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ab6_bad5 WITH (incremental_refresh=true) AS SELECT unnest(ARRAY[g,g]) u, count(*) c FROM ab_gs GROUP BY unnest(ARRAY[g,g])';
    RAISE EXCEPTION 'set-returning function in SELECT should be rejected';
  EXCEPTION WHEN feature_not_supported THEN NULL; END;
  -- a leaf partition used directly, and a plain GROUP BY, must still be accepted
  EXECUTE 'CREATE MATERIALIZED VIEW ab6_ok1 WITH (incremental_refresh=true) AS SELECT grp,count(*) c FROM ab_part_a GROUP BY grp';
  EXECUTE 'CREATE MATERIALIZED VIEW ab6_ok2 WITH (incremental_refresh=true) AS SELECT g,count(*) c FROM ab_gs GROUP BY g';
  EXECUTE 'DROP MATERIALIZED VIEW ab6_ok1'; EXECUTE 'DROP MATERIALIZED VIEW ab6_ok2';
  RAISE NOTICE '6. partitioned/GROUPING SETS/TABLESAMPLE/SRF rejected; leaf + plain GROUP BY accepted: PASS';
END $$;
DROP TABLE ab_part, ab_gs CASCADE;

-- ---------- 7. no-aggregate GROUP BY under parallel aggregation must NOT crash ----------
-- A GROUP BY view with only the key + functionally-dependent bare columns (no
-- explicit aggregate) injects a hidden count(*).  If hasAggs is left false the
-- planner never resolves that Aggref's transition type and a parallel/partial
-- aggregate plan aborts the backend (whole-cluster crash).  hasAggs is now set
-- whenever the count is injected.
DROP TABLE IF EXISTS ab_noagg CASCADE;
CREATE TABLE ab_noagg(id int primary key, dim text, cat text);
INSERT INTO ab_noagg SELECT g,'d'||(g%7),'c'||(g%3) FROM generate_series(1,2000) g;
DO $$
DECLARE d int;
BEGIN
  SET LOCAL debug_parallel_query = on;          -- force the crash-triggering plan
  SET LOCAL min_parallel_table_scan_size = 0;
  EXECUTE 'CREATE MATERIALIZED VIEW ab7_m WITH (incremental_refresh=true) AS SELECT id, dim, cat FROM ab_noagg GROUP BY id';
  EXECUTE 'CREATE MATERIALIZED VIEW ab7_r AS SELECT id, dim, cat FROM ab_noagg GROUP BY id';
  RESET debug_parallel_query; RESET min_parallel_table_scan_size;
  INSERT INTO ab_noagg VALUES (3000,'dX','cX');
  UPDATE ab_noagg SET dim='RN' WHERE id=5;
  DELETE FROM ab_noagg WHERE id=9;
  REFRESH MATERIALIZED VIEW ab7_r;
  SELECT count(*) INTO d FROM (
    (SELECT id,dim,cat FROM ab7_m EXCEPT SELECT id,dim,cat FROM ab7_r)
    UNION ALL (SELECT id,dim,cat FROM ab7_r EXCEPT SELECT id,dim,cat FROM ab7_m)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'no-aggregate GROUP BY: diverged by %', d; END IF;
  RAISE NOTICE '7. no-aggregate GROUP BY survives parallel aggregation + == REFRESH: PASS';
END $$;
DROP MATERIALIZED VIEW ab7_m CASCADE; DROP MATERIALIZED VIEW ab7_r; DROP TABLE ab_noagg CASCADE;
\echo 'PASS: byte-identity audit regressions all green'
