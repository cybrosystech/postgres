-- DBblue IVM — float SUM/AVG rejection.
--
-- Incremental SUM/AVG maintain a running total by adding/subtracting deltas.
-- Floating-point addition is not associative, so a float running total drifts
-- from a true recompute over many deltas.  SUM/AVG over real/double precision
-- must therefore be rejected at CREATE time.  MIN/MAX (comparison only) and
-- COUNT over floats are fine, and numeric/integer SUM/AVG are exact.
\set ON_ERROR_STOP off
\echo ''
\echo '=== DBblue IVM: float aggregate rejection ==='
\echo ''

DROP TABLE IF EXISTS fagg CASCADE;
CREATE TABLE fagg(id serial, g int, r4 real, r8 double precision, n numeric, i int);
INSERT INTO fagg(g,r4,r8,n,i) SELECT g%3, g*1.1, g*1.1, g*1.1, g FROM generate_series(1,30) g;

\echo '--- must be REJECTED ---'
CREATE MATERIALIZED VIEW f_reject1 WITH (incremental_refresh=true) AS
  SELECT g, SUM(r4) FROM fagg GROUP BY g WITH DATA;
CREATE MATERIALIZED VIEW f_reject2 WITH (incremental_refresh=true) AS
  SELECT g, AVG(r8) FROM fagg GROUP BY g WITH DATA;
CREATE MATERIALIZED VIEW f_reject3 WITH (incremental_refresh=true) AS
  SELECT g, SUM(r8 * 2) FROM fagg GROUP BY g WITH DATA;   -- float expression

\echo ''
\echo '--- must be ALLOWED ---'
\set ON_ERROR_STOP on
CREATE MATERIALIZED VIEW f_ok_minmax WITH (incremental_refresh=true) AS
  SELECT g, MIN(r8) mn, MAX(r4) mx, COUNT(*) c FROM fagg GROUP BY g WITH DATA;
CREATE MATERIALIZED VIEW f_ok_numeric WITH (incremental_refresh=true) AS
  SELECT g, SUM(n) s, AVG(n) a FROM fagg GROUP BY g WITH DATA;
CREATE MATERIALIZED VIEW f_ok_int WITH (incremental_refresh=true) AS
  SELECT g, SUM(i) s, AVG(i) a FROM fagg GROUP BY g WITH DATA;

DO $$
DECLARE nrej int; nok int;
BEGIN
    SELECT count(*) INTO nrej FROM pg_class WHERE relname LIKE 'f\_reject%';
    SELECT count(*) INTO nok  FROM pg_class WHERE relname LIKE 'f\_ok\_%';
    IF nrej = 0 THEN RAISE NOTICE 'float SUM/AVG rejected: PASS (0 created)';
    ELSE RAISE EXCEPTION 'float SUM/AVG rejected: FAIL (% slipped through)', nrej; END IF;
    IF nok = 3 THEN RAISE NOTICE 'float MIN/MAX/COUNT + numeric/int SUM/AVG allowed: PASS';
    ELSE RAISE EXCEPTION 'allowed-aggregate check: FAIL (% of 3 created)', nok; END IF;
END $$;

DROP MATERIALIZED VIEW f_ok_minmax;
DROP MATERIALIZED VIEW f_ok_numeric;
DROP MATERIALIZED VIEW f_ok_int;
DROP TABLE fagg CASCADE;
\echo ''
\echo '=== float aggregate rejection test complete ==='
