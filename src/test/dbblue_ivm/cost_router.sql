-- DBblue IVM — cost router (matview_delta_apply + dbblue_ivm_refresh_threshold).
--
-- A statement that changes a large fraction of a source table can cost more to
-- maintain incrementally than to rebuild the matview once.  When the affected
-- row count exceeds dbblue_ivm_refresh_threshold × the source's estimated live
-- tuples (pg_class.reltuples), the trigger falls back to a full REFRESH instead
-- of applying the delta.  This is a PERFORMANCE decision only — both paths
-- produce a matview identical to a full REFRESH — so this test asserts (a)
-- correctness == REFRESH whichever path each delta takes, and (b) the routing
-- itself, observed via the matview's relfilenode: a non-concurrent REFRESH does
-- a heap swap (new relfilenode), incremental in-place DML does not.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: cost router (bulk delta -> full REFRESH) ==='
\echo ''

DROP TABLE IF EXISTS cro CASCADE;
CREATE TABLE cro(id int primary key, k int, a numeric);
INSERT INTO cro SELECT g, g%20, g FROM generate_series(1,5000) g;
ANALYZE cro;                                   -- reltuples must be known for the router
CREATE MATERIALIZED VIEW cro_i WITH (incremental_refresh=true) AS
  SELECT k, sum(a) s, count(*) c FROM cro GROUP BY k;
CREATE MATERIALIZED VIEW cro_o AS
  SELECT k, sum(a) s, count(*) c FROM cro GROUP BY k;

-- ── router ON (default 0.5) ──────────────────────────────────────────────────
SHOW dbblue_ivm_refresh_threshold;

-- small delta (1 row of ~5000 = 0.02% << 0.5): must stay incremental (relfilenode unchanged)
DO $$
DECLARE fn0 oid; fn1 oid;
BEGIN
  fn0 := pg_relation_filenode('cro_i');
  UPDATE cro SET a = a + 1 WHERE id = 42;
  fn1 := pg_relation_filenode('cro_i');
  IF fn0 = fn1 THEN RAISE NOTICE 'small delta stayed incremental (no heap swap): PASS';
  ELSE RAISE EXCEPTION 'small delta unexpectedly refreshed'; END IF;
END$$;

-- bulk delta (UPDATE every row = 100% > 0.5): must fall back to REFRESH (relfilenode changes)
DO $$
DECLARE fn0 oid; fn1 oid;
BEGIN
  fn0 := pg_relation_filenode('cro_i');
  UPDATE cro SET a = a + 10;
  fn1 := pg_relation_filenode('cro_i');
  IF fn0 <> fn1 THEN RAISE NOTICE 'bulk delta routed to full REFRESH (heap swap): PASS';
  ELSE RAISE EXCEPTION 'bulk delta was NOT routed to REFRESH'; END IF;
END$$;

-- bulk INSERT (20000 rows >> 0.5 × 5000): also REFRESH
INSERT INTO cro SELECT 100000+g, g%20, g FROM generate_series(1,20000) g;

REFRESH MATERIALIZED VIEW cro_o;
DO $$DECLARE n int; BEGIN
  SELECT count(*) INTO n FROM ((SELECT k,s,c FROM cro_i EXCEPT SELECT k,s,c FROM cro_o) UNION ALL
                               (SELECT k,s,c FROM cro_o EXCEPT SELECT k,s,c FROM cro_i)) z;
  IF n=0 THEN RAISE NOTICE 'router ON: small + bulk deltas == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'router ON correctness: FAIL (% differ)', n; END IF; END$$;

-- ── router DISABLED (threshold 0): bulk stays incremental, still correct ──────
SET dbblue_ivm_refresh_threshold = 0;
ANALYZE cro;
DO $$
DECLARE fn0 oid; fn1 oid;
BEGIN
  fn0 := pg_relation_filenode('cro_i');
  UPDATE cro SET a = a + 100;                 -- 100% delta, but router off
  fn1 := pg_relation_filenode('cro_i');
  IF fn0 = fn1 THEN RAISE NOTICE 'router disabled: bulk delta stayed incremental: PASS';
  ELSE RAISE EXCEPTION 'router disabled but bulk delta refreshed'; END IF;
END$$;
REFRESH MATERIALIZED VIEW cro_o;
DO $$DECLARE n int; BEGIN
  SELECT count(*) INTO n FROM ((SELECT k,s,c FROM cro_i EXCEPT SELECT k,s,c FROM cro_o) UNION ALL
                               (SELECT k,s,c FROM cro_o EXCEPT SELECT k,s,c FROM cro_i)) z;
  IF n=0 THEN RAISE NOTICE 'router disabled: incremental bulk delta == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'router disabled correctness: FAIL (% differ)', n; END IF; END$$;
RESET dbblue_ivm_refresh_threshold;

DROP TABLE cro CASCADE;
\echo ''
\echo '=== cost router test complete ==='
