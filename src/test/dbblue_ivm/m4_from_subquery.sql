-- m4_from_subquery.sql  (M4.1)
--
-- FROM-clause subquery normalization: a simple FROM-subquery is flattened at
-- CREATE time (MatviewIncrNormalize) into the equivalent flat query the unified
-- engine already maintains.  Correctness spine: a full REFRESH runs the ORIGINAL
-- query through subquery_planner -> pull_up_subqueries, so flattening only what
-- would pull up losslessly and doing the same transform inherits byte-identity.
-- The oracle here is therefore a plain matview over the ORIGINAL (un-flattened)
-- query.
--
-- v1 scope: SOLE-SOURCE FROM-subqueries whose body is a filter/projection over
-- one or more base relations joined by INNER joins (left-deep), no volatile
-- projection.  Everything else — joined-to-a-dimension (outer JOIN RTE above the
-- subquery, deferred), aggregating/DISTINCT inner body, inner outer joins,
-- volatile projection — must be rejected CLEANLY at CREATE, never errored or
-- maintained wrong.

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS m4_o, m4_l, m4_c CASCADE;
CREATE TABLE m4_o(id serial primary key, k int, amt numeric);
CREATE TABLE m4_l(id serial primary key, oid int, qty int);
CREATE TABLE m4_c(id int primary key, region text);
INSERT INTO m4_c SELECT g,'R'||(g%3) FROM generate_series(1,8) g;
INSERT INTO m4_o(k,amt) SELECT 1+(g%8),(g*1.5)::numeric FROM generate_series(1,40) g;
INSERT INTO m4_l(oid,qty) SELECT 1+(g%40),(g%6) FROM generate_series(1,160) g;

-- ---------------------------------------------------------- correctness (accept)
\echo '--- sole-source multi-table INNER-join FROM-subquery == REFRESH of original ---'
CREATE MATERIALIZED VIEW m4_inc WITH (incremental_refresh=true) AS
  SELECT q.k, sum(q.amt) s, sum(q.qty) qy, count(*) n
  FROM (SELECT o.k, o.amt, l.qty FROM m4_o o JOIN m4_l l ON l.oid = o.id) q
  GROUP BY q.k;
CREATE MATERIALIZED VIEW m4_ref AS
  SELECT q.k, sum(q.amt) s, sum(q.qty) qy, count(*) n
  FROM (SELECT o.k, o.amt, l.qty FROM m4_o o JOIN m4_l l ON l.oid = o.id) q
  GROUP BY q.k;

CREATE OR REPLACE FUNCTION pg_temp.m4_diff() RETURNS int LANGUAGE sql AS $$
  SELECT count(*)::int FROM (
    (SELECT k,s,qy,n FROM m4_inc EXCEPT SELECT k,s,qy,n FROM m4_ref)
    UNION ALL (SELECT k,s,qy,n FROM m4_ref EXCEPT SELECT k,s,qy,n FROM m4_inc)) z;
$$;

DO $$
BEGIN
  IF pg_temp.m4_diff() <> 0 THEN RAISE EXCEPTION 'initial: flattened != REFRESH'; END IF;

  INSERT INTO m4_l(oid,qty) VALUES (5,4);              -- extra line
  INSERT INTO m4_o(k,amt) VALUES (7,99.9);            -- order with no line yet
  INSERT INTO m4_l(oid,qty) VALUES (currval('m4_o_id_seq')::int, 2);
  UPDATE m4_o SET amt=amt+1 WHERE id=3;               -- fact value change
  UPDATE m4_l SET qty=qty+10 WHERE id=10;             -- other fact value change
  UPDATE m4_o SET k=2 WHERE id=6;                     -- move a fact between groups
  DELETE FROM m4_l WHERE id=20;                       -- drop a line
  DELETE FROM m4_o WHERE id=4;                        -- drop an order (its lines dangle -> INNER drops them)
  REFRESH MATERIALIZED VIEW m4_ref;

  IF pg_temp.m4_diff() <> 0 THEN RAISE EXCEPTION 'after DML: flattened != REFRESH (% rows)', pg_temp.m4_diff(); END IF;
  RAISE NOTICE 'sole-source multi-table FROM-subquery flattened == REFRESH (initial + mixed DML): PASS';
END $$;
DROP MATERIALIZED VIEW m4_inc, m4_ref;

\echo '--- single-reference CTE with a multi-table INNER-join body rides the same path ---'
CREATE MATERIALIZED VIEW m4_cte WITH (incremental_refresh=true) AS
  WITH q AS (SELECT o.k, o.amt, l.qty FROM m4_o o JOIN m4_l l ON l.oid = o.id)
  SELECT q.k, sum(q.amt) s, count(*) n FROM q GROUP BY q.k;
DROP MATERIALIZED VIEW m4_cte;
\echo 'single-ref multi-table CTE accepted: PASS'

-- ---------------------------------------------------------- clean rejections
\echo '--- shapes that must be rejected cleanly (not errored, not maintained wrong) ---'
DO $$
DECLARE rejected int := 0;
BEGIN
  -- (a) joined to a dimension: outer JOIN RTE above the subquery (deferred)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW m4_x WITH (incremental_refresh=true) AS
             SELECT c.region, sum(q.amt) s
             FROM (SELECT o.id oid, o.k, o.amt, l.qty FROM m4_o o JOIN m4_l l ON l.oid=o.id) q
             LEFT JOIN m4_c c ON c.id = q.k GROUP BY c.region';
    EXECUTE 'DROP MATERIALIZED VIEW m4_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- (b) aggregating inner body (not filter/projection)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW m4_x WITH (incremental_refresh=true) AS
             SELECT q.k, sum(q.s) FROM (SELECT k, sum(amt) s FROM m4_o GROUP BY k) q GROUP BY q.k';
    EXECUTE 'DROP MATERIALIZED VIEW m4_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- (c) inner body contains an OUTER join (v1 requires INNER-only inner)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW m4_x WITH (incremental_refresh=true) AS
             SELECT q.k, count(*) FROM (SELECT o.k, l.qty FROM m4_o o LEFT JOIN m4_l l ON l.oid=o.id) q GROUP BY q.k';
    EXECUTE 'DROP MATERIALIZED VIEW m4_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- (d) volatile projection inside the subquery
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW m4_x WITH (incremental_refresh=true) AS
             SELECT q.k, count(*) FROM (SELECT o.k, random() r FROM m4_o o JOIN m4_l l ON l.oid=o.id) q GROUP BY q.k';
    EXECUTE 'DROP MATERIALIZED VIEW m4_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  IF rejected = 4 THEN RAISE NOTICE 'joined-dim / aggregating / inner-outer-join / volatile all rejected cleanly: PASS';
  ELSE RAISE EXCEPTION 'expected 4 clean rejections, got %', rejected; END IF;
END $$;

DROP FUNCTION pg_temp.m4_diff();
DROP TABLE m4_o, m4_l, m4_c CASCADE;
\echo ''
