-- constfold_sublink.sql
--
-- A trivial constant scalar sublink — "(SELECT <const>)" with no FROM / WHERE /
-- GROUP BY — is folded to the constant before the eligibility gate.  Odoo report
-- models use "(SELECT 1) AS nbr" as a fixed marker column; folding it is
-- byte-identical (a compile-time constant) and clears the query's hasSubLinks so
-- the view is no longer rejected as "subqueries are not supported".  Non-trivial
-- sublinks (correlated, over a table, aggregating, …) are left intact and still
-- rejected cleanly.

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS cf CASCADE;
CREATE TABLE cf(id int primary key, k text, amt numeric);
INSERT INTO cf SELECT g, 'K'||(g%3), g FROM generate_series(1,18) g;

\echo '--- trivial (SELECT const) sublink folded; view maintainable + == REFRESH ---'
CREATE MATERIALIZED VIEW cf_inc WITH (incremental_refresh=true) AS
  SELECT (SELECT 1) AS nbr, k, count(*) c, sum(amt) tot FROM cf GROUP BY k;
CREATE MATERIALIZED VIEW cf_ref AS
  SELECT (SELECT 1) AS nbr, k, count(*) c, sum(amt) tot FROM cf GROUP BY k;
DO $$
DECLARE kind "char"; d int;
BEGIN
  SELECT relkind INTO kind FROM pg_class WHERE relname='cf_inc';
  IF kind <> 'm' THEN RAISE EXCEPTION 'cf_inc should be a plain matview, got %', kind; END IF;
  INSERT INTO cf VALUES (100,'K1',5),(101,'K2',7);
  UPDATE cf SET amt=amt+1 WHERE id=3;
  DELETE FROM cf WHERE id=9;
  REFRESH MATERIALIZED VIEW cf_ref;
  SELECT count(*) INTO d FROM (
    (SELECT nbr,k,c,tot FROM cf_inc EXCEPT SELECT nbr,k,c,tot FROM cf_ref)
    UNION ALL (SELECT nbr,k,c,tot FROM cf_ref EXCEPT SELECT nbr,k,c,tot FROM cf_inc)) z;
  IF d <> 0 THEN RAISE EXCEPTION 'const-folded view diverged from REFRESH by % row(s)', d; END IF;
  RAISE NOTICE 'trivial (SELECT 1) sublink folded, view == REFRESH: PASS';
END $$;
DROP MATERIALIZED VIEW cf_inc, cf_ref;

\echo '--- non-trivial sublinks still rejected cleanly ---'
DO $$
DECLARE rej int := 0;
BEGIN
  -- sublink over a table (not a bare constant)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW cf_x WITH (incremental_refresh=true) AS
             SELECT (SELECT max(amt) FROM cf) AS m, k, count(*) c FROM cf GROUP BY k';
    EXECUTE 'DROP MATERIALIZED VIEW cf_x';
  EXCEPTION WHEN feature_not_supported THEN rej := rej + 1; END;
  -- correlated sublink
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW cf_x WITH (incremental_refresh=true) AS
             SELECT k, count(*) c, (SELECT count(*) FROM cf c2 WHERE c2.k = cf.k) AS n
             FROM cf GROUP BY k';
    EXECUTE 'DROP MATERIALIZED VIEW cf_x';
  EXCEPTION WHEN feature_not_supported THEN rej := rej + 1; END;

  IF rej = 2 THEN RAISE NOTICE 'non-trivial sublinks (table / correlated) still rejected: PASS';
  ELSE RAISE EXCEPTION 'expected 2 rejections, got %', rej; END IF;
END $$;
DROP TABLE cf CASCADE;
\echo ''
