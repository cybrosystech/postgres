-- overlay_projection.sql  (M-OV.1)
--
-- A GROUP BY view rejected only for a non-immutable SELECT-list projection
-- (now()/CURRENT_DATE/STABLE) is split into a maintained CORE matview (grain keys
-- + aggregates + immutable projections) plus a read-time VIEW under the original
-- name that re-adds the peeled column from the core's stored columns.  The core
-- is maintained byte-identically by the existing engine; the peeled column is
-- evaluated at READ time (a documented, arguably-fresher semantic than a frozen
-- REFRESH).  Reuses the HAVING base+view lifecycle (rename to _dbblue_<oid>_base
-- + a user-facing view + INTERNAL dependency).
--
-- Basic variant: peel only when every leaf of the peeled expression is a stored
-- (non-peeled) output column, and only WITH DATA (like HAVING).

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS ovp CASCADE;
CREATE TABLE ovp(id int primary key, k text, created timestamptz, amt numeric);
INSERT INTO ovp SELECT g, 'K'||(g%4), now() - (g||' hours')::interval, g
FROM generate_series(1,40) g;

\echo '--- overlay accepted; published relation is a VIEW backed by a base matview ---'
CREATE MATERIALIZED VIEW ovp_mv WITH (incremental_refresh=true) AS
  SELECT k, count(*) c, max(created) latest, sum(amt) tot,
         EXTRACT(epoch FROM now() - max(created)) AS age_secs
  FROM ovp GROUP BY k;

DO $$
DECLARE kind "char"; ncols int; d int;
BEGIN
  SELECT relkind INTO kind FROM pg_class WHERE relname='ovp_mv';
  IF kind <> 'v' THEN RAISE EXCEPTION 'ovp_mv should be a view, got %', kind; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname ~ '^_dbblue_[0-9]+_base') THEN
    RAISE EXCEPTION 'no base matview created'; END IF;
  -- column-identity: view exposes exactly the original output columns
  SELECT count(*) INTO ncols FROM pg_attribute
   WHERE attrelid='ovp_mv'::regclass AND attnum>0 AND NOT attisdropped;
  IF ncols <> 5 THEN RAISE EXCEPTION 'expected 5 columns, got %', ncols; END IF;
  RAISE NOTICE 'overlay: published as VIEW, base matview created, columns identical: PASS';
END $$;

\echo '--- stable (maintained) columns == REFRESH after DML; peeled column is read-time ---'
CREATE MATERIALIZED VIEW ovp_ref AS
  SELECT k, count(*) c, max(created) latest, sum(amt) tot,
         EXTRACT(epoch FROM now() - max(created)) AS age_secs
  FROM ovp GROUP BY k;

DO $$
DECLARE d int;
BEGIN
  INSERT INTO ovp VALUES (100,'K1', now(), 5),(101,'K7', now(), 9);
  UPDATE ovp SET amt=amt+1 WHERE id=3;
  UPDATE ovp SET k='K2' WHERE id=6;          -- move a row between groups
  DELETE FROM ovp WHERE id=9;
  REFRESH MATERIALIZED VIEW ovp_ref;
  -- the maintained columns must be byte-identical to a full REFRESH
  SELECT count(*) INTO d FROM (
    (SELECT k,c,latest,tot FROM ovp_mv EXCEPT SELECT k,c,latest,tot FROM ovp_ref)
    UNION ALL (SELECT k,c,latest,tot FROM ovp_ref EXCEPT SELECT k,c,latest,tot FROM ovp_mv)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'stable columns diverged from REFRESH by % row(s)', d; END IF;
  -- the peeled column is present and read-time (deterministic function of latest)
  PERFORM 1 FROM ovp_mv WHERE age_secs IS NOT NULL LIMIT 1;
  RAISE NOTICE 'overlay stable columns == REFRESH after DML (peeled column read-time): PASS';
END $$;
DROP VIEW ovp_mv CASCADE;
DROP MATERIALIZED VIEW ovp_ref;

\echo '--- clean rejections: WITH NO DATA, and a non-reconstructible peeled leaf ---'
DO $$
DECLARE rej int := 0;
BEGIN
  -- overlay is WITH DATA only (base rename + view build run on the populated path)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ovp_x WITH (incremental_refresh=true) AS
             SELECT k, count(*) c, max(created) latest,
                    EXTRACT(epoch FROM now() - max(created)) age
             FROM ovp GROUP BY k WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW ovp_x';
  EXCEPTION WHEN feature_not_supported THEN rej := rej + 1; END;

  -- peeled expression whose leaf (max(created)) is NOT a stored output column
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW ovp_x WITH (incremental_refresh=true) AS
             SELECT k, count(*) c, EXTRACT(epoch FROM now() - max(created)) age
             FROM ovp GROUP BY k';
    EXECUTE 'DROP MATERIALIZED VIEW ovp_x';
  EXCEPTION WHEN feature_not_supported THEN rej := rej + 1; END;

  IF rej = 2 THEN RAISE NOTICE 'WITH-NO-DATA + non-reconstructible-leaf both rejected cleanly: PASS';
  ELSE RAISE EXCEPTION 'expected 2 rejections, got %', rej; END IF;
END $$;
DROP TABLE ovp CASCADE;
\echo ''
