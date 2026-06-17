-- DBblue IVM — NULL group-key exclusion (writes are never blocked).
--
-- A NULL value in a GROUP BY/DISTINCT key cannot be maintained incrementally.
-- Instead of blocking the source write or corrupting the matview, the engine
-- injects "<key> IS NOT NULL" into the matview's stored query, so NULL-key rows
-- stay OUTSIDE the matview's scope: the source write always succeeds, the
-- matview excludes those rows, and it stays consistent with a full REFRESH.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: NULL group-key exclusion (writes never blocked) ==='
\echo ''

DROP MATERIALIZED VIEW IF EXISTS nx_mv CASCADE;
DROP TABLE IF EXISTS nx CASCADE;
CREATE TABLE nx(id serial PRIMARY KEY, g int, amt numeric NOT NULL);  -- g nullable
INSERT INTO nx(g,amt) VALUES (1,10),(2,20),(NULL,99);                  -- one NULL key at create
CREATE MATERIALIZED VIEW nx_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s FROM nx GROUP BY g WITH DATA;                   -- auto-filtered to g IS NOT NULL

DO $$
DECLARE nullrows int; mm int; src int;
BEGIN
  -- 1) initial population excludes the NULL-key row
  SELECT count(*) INTO nullrows FROM nx_mv WHERE g IS NULL;
  IF nullrows = 0 THEN RAISE NOTICE 'NULL key excluded at create: PASS';
  ELSE RAISE EXCEPTION 'NULL key excluded at create: FAIL (% null rows)', nullrows; END IF;

  -- 2) inserting a NULL-key row SUCCEEDS (write not blocked) and is ignored
  INSERT INTO nx(g,amt) VALUES (NULL, 555);
  SELECT count(*) INTO src FROM nx WHERE g IS NULL;
  SELECT count(*) INTO nullrows FROM nx_mv WHERE g IS NULL;
  IF src = 2 AND nullrows = 0 THEN RAISE NOTICE 'NULL-key write succeeds + ignored: PASS';
  ELSE RAISE EXCEPTION 'NULL-key write: FAIL (src null=%, mv null=%)', src, nullrows; END IF;

  -- 3) normal maintenance stays consistent with the (non-null) live recompute
  INSERT INTO nx(g,amt) VALUES (1,5);
  DELETE FROM nx WHERE g=2;
  SELECT count(*) INTO mm FROM
    (SELECT g, SUM(amt) s FROM nx WHERE g IS NOT NULL GROUP BY g) l
    FULL JOIN nx_mv m USING (g)
    WHERE l.g IS DISTINCT FROM m.g OR l.s IS DISTINCT FROM m.s;
  IF mm = 0 THEN RAISE NOTICE 'incremental consistency: PASS';
  ELSE RAISE EXCEPTION 'incremental consistency: FAIL (% diff)', mm; END IF;
END $$;

-- 4) incremental result == full REFRESH (the core invariant)
REFRESH MATERIALIZED VIEW nx_mv;
DO $$
DECLARE mm int;
BEGIN
  SELECT count(*) INTO mm FROM
    (SELECT g, SUM(amt) s FROM nx WHERE g IS NOT NULL GROUP BY g) l
    FULL JOIN nx_mv m USING (g)
    WHERE l.g IS DISTINCT FROM m.g OR l.s IS DISTINCT FROM m.s;
  IF mm = 0 THEN RAISE NOTICE 'REFRESH equivalence: PASS';
  ELSE RAISE EXCEPTION 'REFRESH equivalence: FAIL (% diff)', mm; END IF;
END $$;

-- 5) an explicit WHERE g IS NOT NULL is not double-filtered (still correct)
DROP MATERIALIZED VIEW nx_mv;
CREATE MATERIALIZED VIEW nx_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s FROM nx WHERE g IS NOT NULL GROUP BY g WITH DATA;
INSERT INTO nx(g,amt) VALUES (NULL, 1),(3, 30);
DO $$
DECLARE mm int;
BEGIN
  SELECT count(*) INTO mm FROM
    (SELECT g, SUM(amt) s FROM nx WHERE g IS NOT NULL GROUP BY g) l
    FULL JOIN nx_mv m USING (g)
    WHERE l.g IS DISTINCT FROM m.g OR l.s IS DISTINCT FROM m.s;
  IF mm = 0 THEN RAISE NOTICE 'explicit WHERE IS NOT NULL: PASS';
  ELSE RAISE EXCEPTION 'explicit WHERE: FAIL (% diff)', mm; END IF;
END $$;

-- 6) NOT NULL group key: no filter injected, maintenance correct
DROP MATERIALIZED VIEW nx_mv; DROP TABLE nx;
CREATE TABLE nx(id serial PRIMARY KEY, g int NOT NULL, amt numeric NOT NULL);
INSERT INTO nx(g,amt) VALUES (1,10);
CREATE MATERIALIZED VIEW nx_mv WITH (incremental_refresh=true) AS
  SELECT g, SUM(amt) s FROM nx GROUP BY g WITH DATA;
INSERT INTO nx(g,amt) VALUES (1,7),(2,3);
DO $$ DECLARE mm int; BEGIN
  SELECT count(*) INTO mm FROM (SELECT g, SUM(amt) s FROM nx GROUP BY g) l JOIN nx_mv m USING(g) WHERE l.s<>m.s;
  IF mm=0 THEN RAISE NOTICE 'NOT NULL key matview: PASS'; ELSE RAISE EXCEPTION 'NOT NULL key: FAIL'; END IF;
END $$;

DROP MATERIALIZED VIEW nx_mv; DROP TABLE nx;
\echo ''
\echo '=== NULL group-key exclusion test complete ==='
