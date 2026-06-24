-- DBblue IVM — NULL group-key fidelity (writes are never blocked).
--
-- NULL (and partial-NULL, for multi-column keys) group keys are MAINTAINED with
-- full fidelity for the shared-shell shapes (single-table and INNER JOIN
-- aggregates, DISTINCT, HAVING) AND for MIN/MAX.  This works via a NULLS NOT
-- DISTINCT unique index (so a NULL key is one ON CONFLICT arbiter row) plus
-- IS NOT DISTINCT FROM delta key joins (MIN/MAX matches its rescan/affected keys
-- the same way), so the matview equals a full REFRESH including the NULL group.
--
-- Only self-joins still EXCLUDE NULL keys (their recompute path matches keys with
-- =/IN, which NULLs break) — the source write still always succeeds; those rows
-- are simply left out of the matview.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: NULL group-key fidelity ==='
\echo ''

-- 1) Single nullable key, plain aggregate: the NULL group is maintained and the
--    matview equals a full REFRESH (including the NULL group) through DML.
DROP MATERIALIZED VIEW IF EXISTS nx_mv CASCADE;
DROP TABLE IF EXISTS nx CASCADE;
CREATE TABLE nx(id serial PRIMARY KEY, g int, amt numeric NOT NULL);   -- g nullable
INSERT INTO nx(g,amt) VALUES (1,10),(2,20),(NULL,99);
CREATE MATERIALIZED VIEW nx_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s, COUNT(*) c FROM nx GROUP BY g WITH DATA;
DO $$
DECLARE nullrows int; src int; mm int;
BEGIN
  -- NULL group is present (maintained), not excluded
  SELECT count(*) INTO nullrows FROM nx_mv WHERE g IS NULL;
  IF nullrows = 1 THEN RAISE NOTICE 'NULL group maintained at create: PASS';
  ELSE RAISE EXCEPTION 'NULL group at create: FAIL (% null rows)', nullrows; END IF;

  -- NULL-key write SUCCEEDS (never blocked) and updates the NULL group.
  -- Compare with EXCEPT (NULL-safe: set ops treat NULLs as equal), not a join.
  INSERT INTO nx(g,amt) VALUES (NULL,555),(1,5);
  DELETE FROM nx WHERE g=2;
  SELECT count(*) INTO src FROM nx WHERE g IS NULL;
  SELECT count(*) INTO mm FROM (
    (SELECT g, SUM(amt) s, COUNT(*) c FROM nx GROUP BY g EXCEPT SELECT g,s,c FROM nx_mv)
    UNION ALL
    (SELECT g,s,c FROM nx_mv EXCEPT SELECT g, SUM(amt) s, COUNT(*) c FROM nx GROUP BY g)) d;
  IF src = 2 AND mm = 0 THEN RAISE NOTICE 'NULL-key write succeeds + NULL group == live (incl. NULL): PASS';
  ELSE RAISE EXCEPTION 'NULL-key maintenance: FAIL (src null=%, diff=%)', src, mm; END IF;
END $$;
REFRESH MATERIALIZED VIEW nx_mv;
DO $$
DECLARE mm int;
BEGIN
  SELECT count(*) INTO mm FROM (
    (SELECT g, SUM(amt) s, COUNT(*) c FROM nx GROUP BY g EXCEPT SELECT g,s,c FROM nx_mv)
    UNION ALL
    (SELECT g,s,c FROM nx_mv EXCEPT SELECT g, SUM(amt) s, COUNT(*) c FROM nx GROUP BY g)) d;
  IF mm = 0 THEN RAISE NOTICE 'incremental == full REFRESH (incl. NULL group): PASS';
  ELSE RAISE EXCEPTION 'REFRESH equivalence: FAIL (% diff)', mm; END IF;
END $$;
DROP MATERIALIZED VIEW nx_mv; DROP TABLE nx CASCADE;

-- 2) Multi-column key with partial NULLs: each distinct (a,b) — including
--    (5,NULL) and (NULL,NULL) — is its own group, matching a full REFRESH.
DROP TABLE IF EXISTS nm CASCADE;
CREATE TABLE nm(id serial PRIMARY KEY, a int, b int, amt numeric NOT NULL);
INSERT INTO nm(a,b,amt) VALUES (1,2,10),(5,NULL,7),(NULL,NULL,3);
CREATE MATERIALIZED VIEW nm_mv WITH (incremental_refresh=true) AS
  SELECT a, b, SUM(amt) s, COUNT(*) c FROM nm GROUP BY a,b WITH DATA;
INSERT INTO nm(a,b,amt) VALUES (5,NULL,4),(NULL,7,9),(1,2,1);
DELETE FROM nm WHERE a IS NULL AND b IS NULL;
UPDATE nm SET b=NULL WHERE a=1;
DO $$
DECLARE mm int;
BEGIN
  SELECT count(*) INTO mm FROM (
    (SELECT a,b,SUM(amt) s,COUNT(*) c FROM nm GROUP BY a,b
       EXCEPT SELECT a,b,s,c FROM nm_mv)
    UNION ALL
    (SELECT a,b,s,c FROM nm_mv
       EXCEPT SELECT a,b,SUM(amt) s,COUNT(*) c FROM nm GROUP BY a,b)) d;
  IF mm = 0 THEN RAISE NOTICE 'multi-key partial-NULL groups == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'multi-key partial-NULL: FAIL (% diff)', mm; END IF;
END $$;
DROP MATERIALIZED VIEW nm_mv; DROP TABLE nm CASCADE;

-- 3) MIN/MAX: NULL keys are now MAINTAINED with full fidelity (== REFRESH), not
--    excluded — the rescan/delta builders match keys NULL-safely (IS NOT DISTINCT
--    FROM), and the INSERT ON CONFLICT uses the NULLS NOT DISTINCT index.  Covers
--    the rescan path: removing the NULL group's extremum, and a key moving
--    to/from NULL.  (Self-joins still exclude — separate.)
DROP TABLE IF EXISTS mmx CASCADE;
CREATE TABLE mmx(id serial PRIMARY KEY, g int, v numeric);
INSERT INTO mmx(g,v) VALUES (1,5),(NULL,9),(NULL,99);
CREATE MATERIALIZED VIEW mmx_mv WITH (incremental_refresh=true) AS
  SELECT g, MIN(v) mn, MAX(v) mx, COUNT(*) c, SUM(v) s FROM mmx GROUP BY g WITH DATA;
CREATE MATERIALIZED VIEW mmx_ora AS
  SELECT g, MIN(v) mn, MAX(v) mx, COUNT(*) c, SUM(v) s FROM mmx GROUP BY g WITH DATA;
DO $$
DECLARE nullrows int; mm int;
BEGIN
  INSERT INTO mmx(g,v) VALUES (NULL,1),(1,8);    -- NULL-key write must not block
  DELETE FROM mmx WHERE g IS NULL AND v=99;      -- remove NULL group's MAX -> rescan
  UPDATE mmx SET g=NULL WHERE id=1;              -- move a key TO NULL
  REFRESH MATERIALIZED VIEW mmx_ora;
  SELECT count(*) INTO nullrows FROM mmx_mv WHERE g IS NULL;
  SELECT count(*) INTO mm FROM (
    (SELECT g,mn,mx,c,s FROM mmx_mv EXCEPT SELECT g,mn,mx,c,s FROM mmx_ora)
    UNION ALL (SELECT g,mn,mx,c,s FROM mmx_ora EXCEPT SELECT g,mn,mx,c,s FROM mmx_mv)) d;
  IF nullrows = 1 AND mm = 0
  THEN RAISE NOTICE 'MIN/MAX maintains NULL group == REFRESH (rescan + key->NULL): PASS';
  ELSE RAISE EXCEPTION 'MIN/MAX NULL handling: FAIL (nullrows=%, diff=%)', nullrows, mm; END IF;
END $$;
DROP MATERIALIZED VIEW mmx_ora;
DROP MATERIALIZED VIEW mmx_mv; DROP TABLE mmx CASCADE;

-- 4) NOT NULL key: nothing injected, maintenance correct (regression guard).
DROP TABLE IF EXISTS nn CASCADE;
CREATE TABLE nn(id serial PRIMARY KEY, g int NOT NULL, amt numeric NOT NULL);
INSERT INTO nn(g,amt) VALUES (1,10);
CREATE MATERIALIZED VIEW nn_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s FROM nn GROUP BY g WITH DATA;
INSERT INTO nn(g,amt) VALUES (1,7),(2,3);
DO $$ DECLARE mm int; BEGIN
  SELECT count(*) INTO mm FROM (SELECT g, SUM(amt) s FROM nn GROUP BY g) l JOIN nn_mv m USING(g) WHERE l.s<>m.s;
  IF mm=0 THEN RAISE NOTICE 'NOT NULL key matview: PASS'; ELSE RAISE EXCEPTION 'NOT NULL key: FAIL'; END IF;
END $$;
DROP MATERIALIZED VIEW nn_mv; DROP TABLE nn CASCADE;
\echo ''
\echo '=== NULL group-key fidelity test complete ==='
