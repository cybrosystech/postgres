-- DBblue IVM — unsupported aggregate / grouping shapes are rejected cleanly.
--
-- These shapes cannot be maintained by the per-row delta and must be refused
-- at CREATE time with a clear error (never accepted-but-maintained-wrong, and
-- never an internal elog):
--   * COUNT(DISTINCT x) over a SELF-JOIN — single-table / INNER JOIN / outer-join
--                                      DISTINCT is supported (recompute path), and
--                                      DISTINCT + HAVING is supported, but the
--                                      self-join combined-role builder doesn't do
--                                      DISTINCT recompute, so it stays rejected
--   * MIN/MAX (...) FILTER (WHERE …) — hand MIN/MAX builder can't render the
--                                      CASE the filter rewrites to (SUM/COUNT/AVG
--                                      FILTER *are* supported — see filter_aggregates.sql)
--   * GROUP BY <stable expression>  — must be IMMUTABLE (date_trunc over a date
--                                      resolves to the STABLE timestamptz overload)
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
  -- must be REJECTED.  Single-table / INNER JOIN / outer-join COUNT(DISTINCT) and
  -- DISTINCT+HAVING are now supported (recompute path, see distinct_aggregates.sql,
  -- distinct_having.sql, distinct_outer_join.sql); DISTINCT over a SELF-JOIN is not.
  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT a.p, COUNT(DISTINCT a.mt) c FROM uagg a JOIN uagg b ON a.p=b.p GROUP BY a.p WITH DATA')
     THEN RAISE EXCEPTION 'COUNT(DISTINCT) over self-join: FAIL (accepted)'; ELSE RAISE NOTICE 'COUNT(DISTINCT) over self-join: PASS (rejected)'; END IF;

  -- MIN/MAX FILTER stays unsupported (hand builder can't render the CASE the
  -- filter rewrites to); SUM/COUNT/AVG FILTER are supported (filter_aggregates.sql).
  IF _try('CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS SELECT p, MAX(amount) FILTER (WHERE mt=''out_invoice'') mx FROM uagg GROUP BY p WITH DATA')
     THEN RAISE EXCEPTION 'MAX FILTER: FAIL (accepted)'; ELSE RAISE NOTICE 'MAX FILTER: PASS (rejected)'; END IF;

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
