-- DBblue IVM — Phase 1 Route A: NULL group-key runtime guard.
--
-- A NULL value in a GROUP BY/DISTINCT key cannot be maintained incrementally.
-- Instead of silently creating duplicate, never-cleaned rows, the engine now
-- raises a clear error when an insert delta would introduce a NULL key — which
-- rolls the whole statement back (no corruption).  NOT-NULL-by-schema keys pay
-- nothing.
\set ON_ERROR_STOP off
\echo ''
\echo '=== DBblue IVM: NULL group-key guard ==='
\echo ''

DROP MATERIALIZED VIEW IF EXISTS nk_mv CASCADE;
DROP TABLE IF EXISTS nk CASCADE;
CREATE TABLE nk(id serial PRIMARY KEY, g int, amt numeric NOT NULL);   -- g nullable by schema
INSERT INTO nk(g,amt) VALUES (1,10),(2,20);
CREATE MATERIALIZED VIEW nk_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s FROM nk GROUP BY g WITH DATA;

DO $$
DECLARE blocked bool := false; nullrows int; gsum numeric;
BEGIN
  -- 1) inserting a NULL group key must raise (and roll back)
  BEGIN
    INSERT INTO nk(g,amt) VALUES (NULL, 99);
  EXCEPTION WHEN feature_not_supported THEN
    blocked := true;
  END;
  IF blocked THEN RAISE NOTICE 'NULL-key insert blocked: PASS';
  ELSE RAISE EXCEPTION 'NULL-key insert blocked: FAIL (was allowed)'; END IF;

  -- 2) matview must be unchanged (no NULL row leaked)
  SELECT count(*) INTO nullrows FROM nk_mv WHERE g IS NULL;
  IF nullrows = 0 THEN RAISE NOTICE 'no NULL row leaked: PASS';
  ELSE RAISE EXCEPTION 'no NULL row leaked: FAIL (% null rows)', nullrows; END IF;

  -- 3) normal (non-null) maintenance still works
  INSERT INTO nk(g,amt) VALUES (1,5);
  SELECT s INTO gsum FROM nk_mv WHERE g=1;
  IF gsum = 15 THEN RAISE NOTICE 'normal insert still works: PASS';
  ELSE RAISE EXCEPTION 'normal insert: FAIL (g=1 sum=%, want 15)', gsum; END IF;

  -- 4) a batch containing one NULL key fails atomically (nothing applied)
  blocked := false;
  BEGIN
    INSERT INTO nk(g,amt) VALUES (3,1),(NULL,2),(4,3);
  EXCEPTION WHEN feature_not_supported THEN
    blocked := true;
  END;
  SELECT count(*) INTO nullrows FROM nk_mv WHERE g IN (3,4);
  IF blocked AND nullrows = 0 THEN RAISE NOTICE 'mixed batch atomic rollback: PASS';
  ELSE RAISE EXCEPTION 'mixed batch: FAIL (blocked=%, leaked=%)', blocked, nullrows; END IF;
END $$;

\set ON_ERROR_STOP on
-- 5) NOT NULL group key: guard is a no-op, maintenance correct
DROP MATERIALIZED VIEW nk_mv; DROP TABLE nk;
CREATE TABLE nk(id serial PRIMARY KEY, g int NOT NULL, amt numeric NOT NULL);
INSERT INTO nk(g,amt) VALUES (1,10);
CREATE MATERIALIZED VIEW nk_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s FROM nk GROUP BY g WITH DATA;
INSERT INTO nk(g,amt) VALUES (1,7),(2,3);
DO $$ DECLARE mm int; BEGIN
  SELECT count(*) INTO mm FROM (SELECT g,SUM(amt) s FROM nk GROUP BY g) l JOIN nk_mv m USING(g) WHERE l.s<>m.s;
  IF mm=0 THEN RAISE NOTICE 'NOT NULL key matview: PASS'; ELSE RAISE EXCEPTION 'NOT NULL key: FAIL'; END IF;
END $$;

DROP MATERIALIZED VIEW nk_mv; DROP TABLE nk;
\echo ''
\echo '=== NULL group-key guard test complete ==='
