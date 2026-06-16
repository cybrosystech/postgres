-- DBblue IVM — correctness for aliased group columns and NULL aggregate args.
--
-- Regression for two bugs found in audit:
--   A) MIN/MAX with an aliased group column (SELECT g AS k ...) failed on
--      DELETE/UPDATE ("column k does not exist") because the rescan SQL mixed
--      the source column name and the output alias.
--   B) A NULL aggregate argument corrupted the running SUM/AVG to NULL
--      (running + NULL = NULL); now maintained NULL-safely.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: aliased-group + NULL-arg correctness ==='
\echo ''

DROP MATERIALIZED VIEW IF EXISTS na_mv CASCADE;
DROP TABLE IF EXISTS na CASCADE;
CREATE TABLE na(id serial PRIMARY KEY, g int, amt numeric);

-- helper: compare matview to a live recompute of <live>, on column list <cols>
CREATE OR REPLACE FUNCTION _cmp(cols text, live text) RETURNS int LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
  EXECUTE format('SELECT (SELECT count(*) FROM (SELECT %1$s FROM (%2$s) l EXCEPT SELECT %1$s FROM na_mv) a)
                       + (SELECT count(*) FROM (SELECT %1$s FROM na_mv EXCEPT SELECT %1$s FROM (%2$s) r) b)',
                 cols, live) INTO n;
  RETURN n;
END $$;

\echo '--- A) aliased MIN/MAX full lifecycle ---'
INSERT INTO na(g,amt) VALUES (1,5),(1,5),(1,10),(2,3);
CREATE MATERIALIZED VIEW na_mv WITH (incremental_refresh=true) AS
  SELECT g AS k, MIN(amt) mn, MAX(amt) mx, COUNT(*) c FROM na GROUP BY g WITH DATA;
DELETE FROM na WHERE id=(SELECT min(id) FROM na WHERE g=1 AND amt=5);
INSERT INTO na(g,amt) VALUES (1,99),(3,7);
UPDATE na SET g=5 WHERE g=1 AND amt=10;
UPDATE na SET amt=1 WHERE g=1 AND amt=99;
DELETE FROM na WHERE g=2;
DO $$ DECLARE n int; BEGIN
  n := _cmp('k,mn,mx,c', 'SELECT g AS k, MIN(amt) mn, MAX(amt) mx, COUNT(*) c FROM na GROUP BY g');
  IF n=0 THEN RAISE NOTICE 'aliased MIN/MAX lifecycle: PASS'; ELSE RAISE EXCEPTION 'aliased MIN/MAX: FAIL (% diff)', n; END IF;
END $$;

\echo '--- B) NULL aggregate argument: SUM/AVG stay correct ---'
DROP MATERIALIZED VIEW na_mv; DELETE FROM na;
INSERT INTO na(g,amt) VALUES (1,10),(1,20),(2,5);
CREATE MATERIALIZED VIEW na_mv WITH (incremental_refresh=true) AS
  SELECT g AS k, SUM(amt) s, AVG(amt) a, COUNT(*) c FROM na GROUP BY g WITH DATA;
INSERT INTO na(g,amt) VALUES (1,NULL);        -- NULL arg: sum/avg unchanged, count+1
INSERT INTO na(g,amt) VALUES (2,NULL),(2,NULL);
DELETE FROM na WHERE g=1 AND amt=10;          -- remove a real value
INSERT INTO na(g,amt) VALUES (3,NULL);        -- brand-new group, only NULL so far
INSERT INTO na(g,amt) VALUES (3,100);         -- then a real value: sum must be 100
DO $$ DECLARE n int; BEGIN
  n := _cmp('k,s,a,c', 'SELECT g AS k, SUM(amt) s, AVG(amt) a, COUNT(*) c FROM na GROUP BY g');
  IF n=0 THEN RAISE NOTICE 'NULL-arg SUM/AVG: PASS'; ELSE RAISE EXCEPTION 'NULL-arg SUM/AVG: FAIL (% diff)', n; END IF;
END $$;

DROP FUNCTION _cmp(text,text);
DROP MATERIALIZED VIEW na_mv;
DROP TABLE na;
\echo ''
\echo '=== aliased-group + NULL-arg correctness test complete ==='
