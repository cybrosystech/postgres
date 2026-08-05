-- DBblue IVM — DDL on source tables.
--
-- Schema changes to a tracked source table must never silently corrupt or
-- silently disable an incremental matview.  PostgreSQL's column-level
-- dependencies already block the dangerous DROP/ALTER-TYPE cases; the engine
-- additionally blocks renaming a column an incremental matview uses (its delta
-- SQL is keyed by column name).  ADD COLUMN and changes to unused columns are
-- allowed and leave the matview correct.
\set ON_ERROR_STOP off
\echo ''
\echo '=== DBblue IVM: DDL on source tables ==='
\echo ''

DROP TABLE IF EXISTS ddl_src CASCADE;
CREATE TABLE ddl_src(id serial PRIMARY KEY, g int, amt numeric, extra text);
INSERT INTO ddl_src(g,amt,extra) SELECT i%4, i, 'x' FROM generate_series(1,40) i;
CREATE MATERIALIZED VIEW ddl_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) tot, COUNT(*) c FROM ddl_src GROUP BY g WITH DATA;

\echo '--- ADD COLUMN: allowed, matview stays incremental + consistent ---'
ALTER TABLE ddl_src ADD COLUMN newc int;
INSERT INTO ddl_src(g,amt) VALUES (0, 1000);
DO $$ BEGIN
  IF (SELECT tot FROM ddl_mv WHERE g=0) = (SELECT SUM(amt) FROM ddl_src WHERE g=0)
  THEN RAISE NOTICE 'ADD COLUMN: PASS (still incremental)';
  ELSE RAISE EXCEPTION 'ADD COLUMN: FAIL (matview stale)'; END IF;
END $$;

\echo '--- RENAME used column: must be BLOCKED ---'
DO $$ BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE ddl_src RENAME COLUMN amt TO amt2';
    RAISE EXCEPTION 'RENAME used column: FAIL (was allowed)';
  EXCEPTION WHEN feature_not_supported THEN
    RAISE NOTICE 'RENAME used column: PASS (blocked)';
  END;
END $$;

\echo '--- RENAME GROUP BY column: must be BLOCKED ---'
DO $$ BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE ddl_src RENAME COLUMN g TO grp';
    RAISE EXCEPTION 'RENAME group column: FAIL (was allowed)';
  EXCEPTION WHEN feature_not_supported THEN
    RAISE NOTICE 'RENAME group column: PASS (blocked)';
  END;
END $$;

\echo '--- RENAME unused column: allowed ---'
DO $$ BEGIN
  ALTER TABLE ddl_src RENAME COLUMN extra TO extra2;
  RAISE NOTICE 'RENAME unused column: PASS (allowed)';
END $$;

\echo '--- DROP used column: blocked by dependency, with incremental hint ---'
DO $$ DECLARE h text; BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE ddl_src DROP COLUMN amt';
    RAISE EXCEPTION 'DROP used column: FAIL (was allowed)';
  EXCEPTION WHEN dependent_objects_still_exist THEN
    GET STACKED DIAGNOSTICS h = PG_EXCEPTION_HINT;
    IF h ILIKE '%incremental materialized view%'
    THEN RAISE NOTICE 'DROP used column: PASS (blocked, incremental hint)';
    ELSE RAISE EXCEPTION 'DROP used column: blocked but hint missing incremental wording: %', h; END IF;
  END;
END $$;

\echo '--- ALTER TYPE of used column: blocked ---'
DO $$ BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE ddl_src ALTER COLUMN amt TYPE bigint';
    RAISE EXCEPTION 'ALTER TYPE used column: FAIL (was allowed)';
  EXCEPTION WHEN dependent_objects_still_exist OR feature_not_supported THEN
    RAISE NOTICE 'ALTER TYPE used column: PASS (blocked)';
  END;
END $$;

\echo '--- DROP TABLE source: blocked by dependency, with incremental hint ---'
DO $$ DECLARE h text; BEGIN
  BEGIN
    EXECUTE 'DROP TABLE ddl_src';
    RAISE EXCEPTION 'DROP TABLE source: FAIL (was allowed)';
  EXCEPTION WHEN dependent_objects_still_exist THEN
    GET STACKED DIAGNOSTICS h = PG_EXCEPTION_HINT;
    IF h ILIKE '%incremental materialized view%'
    THEN RAISE NOTICE 'DROP TABLE source: PASS (blocked, incremental hint)';
    ELSE RAISE EXCEPTION 'DROP TABLE source: blocked but hint missing incremental wording: %', h; END IF;
  END;
END $$;

-- writes still work and matview is still consistent after all the blocked DDL
INSERT INTO ddl_src(g,amt) VALUES (1, 500);
DO $$ BEGIN
  IF (SELECT tot FROM ddl_mv WHERE g=1) = (SELECT SUM(amt) FROM ddl_src WHERE g=1)
  THEN RAISE NOTICE 'post-DDL writes: PASS (matview consistent)';
  ELSE RAISE EXCEPTION 'post-DDL writes: FAIL'; END IF;
END $$;

DROP MATERIALIZED VIEW ddl_mv;
DROP TABLE ddl_src;
\echo ''
\echo '=== DDL-on-source test complete ==='
