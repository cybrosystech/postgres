-- DBblue IVM — GROUP BY on an expression (e.g. date_trunc('month', d)).
--
-- The grouping key is a deterministic (IMMUTABLE) expression rather than a bare
-- column.  The deparse core copies the view Query — grouping expressions and all
-- — and only swaps the source RTE for the transition table, so ruleutils renders
-- the same GROUP BY over the changed rows; every consumer keys on the matview
-- OUTPUT column that stores the expression value.  Expression grouping is
-- auto-routed to deparse regardless of the GUC.
--
-- Every shape is checked == a stock full REFRESH (the 100%-guaranteed oracle)
-- via EXCEPT both directions, after a mix of INSERT/DELETE/UPDATE, on BOTH the
-- deparse-default and GUC-off paths (expression grouping must force deparse
-- either way).  Negative cases assert clean rejection.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: GROUP BY expression ==='
\echo ''

-- helper: build an incremental + a normal matview for the same definition,
-- apply DML to the source, REFRESH the normal one, return the symmetric diff.
CREATE OR REPLACE FUNCTION _gbe(deparse bool) RETURNS int LANGUAGE plpgsql AS $$
DECLARE ndiff int := 0; t int;
BEGIN
  EXECUTE 'SET dbblue_ivm_deparse_delta = ' || deparse::text;

  ----------------------------------------------------------------- single table
  DROP TABLE IF EXISTS gbe_s CASCADE;
  CREATE TABLE gbe_s(id serial primary key, d timestamp, amt numeric, cat int);
  INSERT INTO gbe_s(d,amt,cat)
    SELECT '2024-01-01'::timestamp + (g||' days')::interval, g, g%4
    FROM generate_series(0,120) g;
  -- NULL group key: date_trunc(NULL) = NULL must be kept as its own group
  INSERT INTO gbe_s(d,amt,cat) VALUES (NULL, 7, 1), (NULL, 9, 2);

  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c, AVG(amt) a
    FROM gbe_s GROUP BY date_trunc('month', d);
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c, AVG(amt) a
    FROM gbe_s GROUP BY date_trunc('month', d);

  INSERT INTO gbe_s(d,amt,cat) VALUES ('2024-03-15',1000,0),('2024-07-01',50,3);
  INSERT INTO gbe_s(d,amt,cat) VALUES (NULL, 100, 0);     -- grow NULL group
  DELETE FROM gbe_s WHERE amt IN (5, 7);                  -- incl. one NULL-key row
  UPDATE gbe_s SET d = d + interval '40 days' WHERE amt = 10;  -- moves group
  UPDATE gbe_s SET amt = amt + 1 WHERE cat = 2;

  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT m,s,c,a FROM gbe_i EXCEPT SELECT m,s,c,a FROM gbe_n)
    UNION ALL (SELECT m,s,c,a FROM gbe_n EXCEPT SELECT m,s,c,a FROM gbe_i)) d;
  ndiff := ndiff + t;
  DROP MATERIALIZED VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  ------------------------------------------------ arithmetic / mod expression key
  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT (cat % 3) AS b, SUM(amt) s, COUNT(*) c FROM gbe_s GROUP BY (cat % 3);
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT (cat % 3) AS b, SUM(amt) s, COUNT(*) c FROM gbe_s GROUP BY (cat % 3);
  INSERT INTO gbe_s(d,amt,cat) VALUES ('2024-02-02', 42, 5);
  DELETE FROM gbe_s WHERE amt = 9;
  UPDATE gbe_s SET cat = cat + 1 WHERE id % 7 = 0;
  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT b,s,c FROM gbe_i EXCEPT SELECT b,s,c FROM gbe_n)
    UNION ALL (SELECT b,s,c FROM gbe_n EXCEPT SELECT b,s,c FROM gbe_i)) d;
  ndiff := ndiff + t;
  DROP MATERIALIZED VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  ------------------------------------- mixed: plain column + expression group keys
  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT cat, date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c
    FROM gbe_s GROUP BY cat, date_trunc('month', d);
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT cat, date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c
    FROM gbe_s GROUP BY cat, date_trunc('month', d);
  INSERT INTO gbe_s(d,amt,cat) VALUES ('2024-09-09', 11, 2);
  DELETE FROM gbe_s WHERE id % 11 = 0;
  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT cat,m,s,c FROM gbe_i EXCEPT SELECT cat,m,s,c FROM gbe_n)
    UNION ALL (SELECT cat,m,s,c FROM gbe_n EXCEPT SELECT cat,m,s,c FROM gbe_i)) d;
  ndiff := ndiff + t;
  DROP MATERIALIZED VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  ------------------------------------------------------- HAVING over expression key
  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c
    FROM gbe_s GROUP BY date_trunc('month', d) HAVING COUNT(*) > 5;
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c
    FROM gbe_s GROUP BY date_trunc('month', d) HAVING COUNT(*) > 5;
  INSERT INTO gbe_s(d,amt,cat) SELECT '2024-11-01', g, 0 FROM generate_series(1,8) g;
  DELETE FROM gbe_s WHERE d < '2024-01-05';   -- shrink a group below threshold
  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT m,s,c FROM gbe_i EXCEPT SELECT m,s,c FROM gbe_n)
    UNION ALL (SELECT m,s,c FROM gbe_n EXCEPT SELECT m,s,c FROM gbe_i)) d;
  ndiff := ndiff + t;
  -- HAVING matview: the original name is a VIEW over the hidden _base matview;
  -- DROP VIEW runs the teardown hook that removes the base + catalog + triggers.
  DROP VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  -------------------------------- HAVING that references the GROUP BY EXPRESSION
  -- Regressions (all once raised internal errors in the HAVING recompute deparse):
  --   * non-Var group key in HAVING (bound to a base column instead of output col)
  --   * NullTest:  HAVING <expr> IS NOT NULL
  --   * multi-arg / real function:  power(...), abs(...)  (1-arg was mis-cast)
  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c
    FROM gbe_s GROUP BY date_trunc('month', d)
    HAVING date_trunc('month', d) >= '2024-02-01' AND COUNT(*) > 2
       AND date_trunc('month', d) IS NOT NULL AND SUM(amt) IS NOT NULL;
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT date_trunc('month', d) AS m, SUM(amt) s, COUNT(*) c
    FROM gbe_s GROUP BY date_trunc('month', d)
    HAVING date_trunc('month', d) >= '2024-02-01' AND COUNT(*) > 2
       AND date_trunc('month', d) IS NOT NULL AND SUM(amt) IS NOT NULL;
  INSERT INTO gbe_s(d,amt,cat) VALUES ('2024-02-03', 13, 1),('2023-12-31', 4, 0);
  DELETE FROM gbe_s WHERE amt = 88;
  UPDATE gbe_s SET d = '2024-02-10' WHERE id % 9 = 0;
  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT m,s,c FROM gbe_i EXCEPT SELECT m,s,c FROM gbe_n)
    UNION ALL (SELECT m,s,c FROM gbe_n EXCEPT SELECT m,s,c FROM gbe_i)) d;
  ndiff := ndiff + t;
  DROP VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  ---------------------------- HAVING with a multi-arg / 1-arg real function (bug2)
  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT (cat % 5) AS r, SUM(amt) s, COUNT(*) c FROM gbe_s GROUP BY (cat % 5)
    HAVING power((cat % 5)::numeric, 2) > 1 AND abs(cat % 5) >= 0;
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT (cat % 5) AS r, SUM(amt) s, COUNT(*) c FROM gbe_s GROUP BY (cat % 5)
    HAVING power((cat % 5)::numeric, 2) > 1 AND abs(cat % 5) >= 0;
  INSERT INTO gbe_s(d,amt,cat) VALUES ('2024-05-05', 17, 6);
  DELETE FROM gbe_s WHERE amt = 13;
  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT r,s,c FROM gbe_i EXCEPT SELECT r,s,c FROM gbe_n)
    UNION ALL (SELECT r,s,c FROM gbe_n EXCEPT SELECT r,s,c FROM gbe_i)) d;
  ndiff := ndiff + t;
  DROP VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  --------------------------------------------- INNER JOIN + expression group key
  DROP TABLE IF EXISTS gbe_o CASCADE;
  CREATE TABLE gbe_o(id int primary key, region text);
  INSERT INTO gbe_o SELECT g, 'r'||(g%3) FROM generate_series(0,4) g;
  CREATE MATERIALIZED VIEW gbe_i WITH (incremental_refresh=true) AS
    SELECT o.region, date_trunc('month', s.d) AS m, SUM(s.amt) tot, COUNT(*) c
    FROM gbe_s s JOIN gbe_o o ON o.id = s.cat
    GROUP BY o.region, date_trunc('month', s.d);
  CREATE MATERIALIZED VIEW gbe_n AS
    SELECT o.region, date_trunc('month', s.d) AS m, SUM(s.amt) tot, COUNT(*) c
    FROM gbe_s s JOIN gbe_o o ON o.id = s.cat
    GROUP BY o.region, date_trunc('month', s.d);
  INSERT INTO gbe_s(d,amt,cat) VALUES ('2024-06-06', 77, 1), ('2024-06-07', 88, 3);
  DELETE FROM gbe_s WHERE amt = 42;
  UPDATE gbe_o SET region = 'rX' WHERE id = 1;   -- join-side change
  REFRESH MATERIALIZED VIEW gbe_n;
  SELECT count(*) INTO t FROM (
    (SELECT region,m,tot,c FROM gbe_i EXCEPT SELECT region,m,tot,c FROM gbe_n)
    UNION ALL (SELECT region,m,tot,c FROM gbe_n EXCEPT SELECT region,m,tot,c FROM gbe_i)) d;
  ndiff := ndiff + t;
  DROP MATERIALIZED VIEW gbe_i; DROP MATERIALIZED VIEW gbe_n;

  DROP TABLE gbe_s CASCADE; DROP TABLE gbe_o CASCADE;
  RETURN ndiff;
END $$;

DO $$
BEGIN
  IF _gbe(true) = 0 AND _gbe(false) = 0 THEN
    RAISE NOTICE 'GROUP BY expression == REFRESH on both paths '
                 '(single/arith/mixed/HAVING/JOIN, incl. NULL key): PASS';
  ELSE
    RAISE EXCEPTION 'GROUP BY expression fidelity: FAIL';
  END IF;
END $$;
DROP FUNCTION _gbe(bool);
RESET dbblue_ivm_deparse_delta;

-- ------------------------------------------------------------------ rejections
\echo '--- negative cases (must be rejected) ---'
DROP TABLE IF EXISTS gbe_r CASCADE;
CREATE TABLE gbe_r(id serial primary key, d timestamp, amt numeric, k int);
INSERT INTO gbe_r(d,amt,k) SELECT '2024-01-01'::timestamp+(g||' days')::interval, g, g%3 FROM generate_series(0,30) g;

DO $$
DECLARE rejected int := 0;
BEGIN
  -- volatile group expression
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW gbe_x WITH (incremental_refresh=true) AS
             SELECT (random()*3)::int b, COUNT(*) FROM gbe_r GROUP BY (random()*3)::int';
    EXECUTE 'DROP MATERIALIZED VIEW gbe_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- grouping expression not in the SELECT list
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW gbe_x WITH (incremental_refresh=true) AS
             SELECT SUM(amt) FROM gbe_r GROUP BY date_trunc(''month'', d)';
    EXECUTE 'DROP MATERIALIZED VIEW gbe_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- MIN/MAX with an expression group key (deparse path does not build MIN/MAX)
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW gbe_x WITH (incremental_refresh=true) AS
             SELECT date_trunc(''month'', d) m, MIN(amt) mn FROM gbe_r GROUP BY date_trunc(''month'', d)';
    EXECUTE 'DROP MATERIALIZED VIEW gbe_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  -- user output column collides with the reserved __mv_ prefix
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW gbe_x WITH (incremental_refresh=true) AS
             SELECT (k % 4) AS "__mv_count__", COUNT(*) c FROM gbe_r GROUP BY (k % 4)';
    EXECUTE 'DROP MATERIALIZED VIEW gbe_x';
  EXCEPTION WHEN feature_not_supported THEN rejected := rejected + 1; END;

  IF rejected = 4 THEN RAISE NOTICE 'volatile / unselected / MIN+expr / reserved-name all rejected: PASS';
  ELSE RAISE EXCEPTION 'expected 4 rejections, got %', rejected; END IF;
END $$;
DROP TABLE gbe_r CASCADE;
\echo ''
\echo '=== GROUP BY expression test complete ==='
