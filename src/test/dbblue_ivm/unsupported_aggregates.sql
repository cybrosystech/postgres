-- DBblue IVM — unsupported aggregate / grouping shapes are rejected cleanly.
--
-- These shapes cannot be maintained by the per-row delta and must be refused
-- at CREATE time with a clear error (never accepted-but-maintained-wrong, and
-- never an internal elog):
--   * COUNT(DISTINCT x)            — needs per-value occurrence tracking
--   * agg(...) FILTER (WHERE ...)  — filter not yet honored by the delta SQL
--   * GROUP BY <expression>        — delta builders key on plain columns
-- Supported shapes alongside them must still be accepted.
\set ON_ERROR_STOP off
\echo ''
\echo '=== DBblue IVM: unsupported aggregate/grouping rejection ==='
\echo ''

DROP TABLE IF EXISTS uagg CASCADE;
CREATE TABLE uagg(id serial PRIMARY KEY, p int, mt text, amount numeric, d date);
INSERT INTO uagg(p,mt,amount,d) SELECT i%5,'out_invoice',i,'2024-01-01'::date+i FROM generate_series(1,30) i;

-- helper: returns 't' if the matview was created, 'f' if rejected
CREATE OR REPLACE FUNCTION _try(sql text) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE sql;
  EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS _m CASCADE';
  EXECUTE 'DROP VIEW IF EXISTS _m CASCADE';
  RETURN true;
EXCEPTION WHEN feature_not_supported THEN
  RETURN false;
END $$;

DO $$
BEGIN
  -- must be REJECTED
  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT p, COUNT(DISTINCT mt) c FROM uagg GROUP BY p WITH DATA')
     THEN RAISE EXCEPTION 'COUNT(DISTINCT): FAIL (accepted)'; ELSE RAISE NOTICE 'COUNT(DISTINCT): PASS (rejected)'; END IF;

  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT p, COUNT(*) FILTER (WHERE mt=''out_invoice'') c FROM uagg GROUP BY p WITH DATA')
     THEN RAISE EXCEPTION 'COUNT FILTER: FAIL (accepted)'; ELSE RAISE NOTICE 'COUNT(*) FILTER: PASS (rejected)'; END IF;

  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT p, SUM(amount) FILTER (WHERE mt=''out_invoice'') s FROM uagg GROUP BY p WITH DATA')
     THEN RAISE EXCEPTION 'SUM FILTER: FAIL (accepted)'; ELSE RAISE NOTICE 'SUM FILTER: PASS (rejected)'; END IF;

  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT date_trunc(''month'',d) m, SUM(amount) s FROM uagg GROUP BY date_trunc(''month'',d) WITH DATA')
     THEN RAISE EXCEPTION 'GROUP BY expr: FAIL (accepted)'; ELSE RAISE NOTICE 'GROUP BY expression: PASS (rejected)'; END IF;

  -- must be ACCEPTED
  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT p, SUM(amount) s, COUNT(*) c, AVG(amount) a, MIN(amount) mn, MAX(amount) mx FROM uagg GROUP BY p WITH DATA')
     THEN RAISE NOTICE 'plain SUM/COUNT/AVG/MIN/MAX: PASS (accepted)'; ELSE RAISE EXCEPTION 'plain aggregates: FAIL (rejected)'; END IF;

  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT p, mt, SUM(amount*2) s FROM uagg GROUP BY p, mt WITH DATA')
     THEN RAISE NOTICE 'multi-col GROUP BY + arith arg: PASS (accepted)'; ELSE RAISE EXCEPTION 'multi-col/arith: FAIL (rejected)'; END IF;
END $$;

DROP FUNCTION _try(text);
DROP TABLE uagg CASCADE;
\echo ''
\echo '=== unsupported-aggregate rejection test complete ==='
