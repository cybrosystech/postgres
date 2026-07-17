-- independent_duplicate_selfjoin.sql
--
-- A table appearing TWICE in a query is only a self-join when the two aliases are
-- connected by a join qual (e.g. t1.parent = t2.id).  When they sit on INDEPENDENT
-- branches (no qual mentioning both) they are just two dimension lookups — the
-- Odoo vendor_delay_report pattern: uom_uom joined once via the product template
-- (INNER) and once via the stock move line (LEFT), feeding one aggregate.  Such a
-- shape combined with an outer join used to be rejected as a "self-join + outer
-- join"; it is now maintained by the general recompute with a per-role UNION
-- affected-set (incr_build_recompute_sql_multirole).
--
-- The load-bearing correctness point: a delta on the doubled table reaches the
-- affected groups through EITHER role, so arm 1 must be UNIONed over BOTH varnos.
-- Capturing only the first role silently misses groups reachable only through the
-- other — the "ml-only role" case below, which a differential on typical data
-- (where every move-line's unit matches its template) would NOT exercise.

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS idp_uom, idp_prod, idp_fct, idp_ml CASCADE;
CREATE TABLE idp_uom(id int primary key, factor numeric);
CREATE TABLE idp_prod(id int primary key, tmpl_uom int);
CREATE TABLE idp_fct(id int primary key, prod_id int);
CREATE TABLE idp_ml(id int primary key, fact_id int, ml_uom int, quantity numeric);
INSERT INTO idp_uom VALUES (1,1.0),(2,2.0),(3,4.0),(4,5.0);
INSERT INTO idp_prod VALUES (10,1),(11,2);
INSERT INTO idp_fct VALUES (100,10),(101,11),(102,10);
INSERT INTO idp_ml VALUES (1000,100,3,8.0),   -- ml_uom 3 != template uom 1  (ml-only)
                          (1001,101,2,10.0),  -- ml_uom 2 == template uom 2  (shared)
                          (1002,102,4,20.0);  -- ml_uom 4 != template uom 1  (ml-only)

-- ---------------------------------------------------------------- acceptance
\echo '--- gate: independent duplicate accepted; real self-join+outer rejected; optional key rejected ---'
DO $$
DECLARE ok int := 0;
BEGIN
  -- independent duplicate (uom twice, unconnected), GROUP BY on the anchor: ACCEPT
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW idp_x WITH (incremental_refresh=true) AS
             SELECT m.id, sum(CASE WHEN ml.quantity IS NOT NULL
                                   THEN ml.quantity/ml_uom.factor*pt_uom.factor ELSE 0 END) c
             FROM idp_fct m JOIN idp_prod p ON p.id=m.prod_id
               JOIN idp_uom pt_uom ON pt_uom.id=p.tmpl_uom
               LEFT JOIN idp_ml ml ON ml.fact_id=m.id
               LEFT JOIN idp_uom ml_uom ON ml_uom.id=ml.ml_uom
             GROUP BY m.id WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW idp_x'; ok := ok + 1;
  EXCEPTION WHEN feature_not_supported THEN NULL; END;

  -- GENUINE self-join (t1.id = t2.parent connects the two aliases) + outer join: REJECT
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW idp_x WITH (incremental_refresh=true) AS
             SELECT a.id, count(*) c FROM idp_uom a JOIN idp_uom b ON b.id = a.factor::int
               LEFT JOIN idp_prod p ON p.tmpl_uom = a.id GROUP BY a.id, b.id, b.factor';
    EXECUTE 'DROP MATERIALIZED VIEW idp_x';
  EXCEPTION WHEN feature_not_supported THEN ok := ok + 1; END;

  -- independent duplicate but a GROUP BY key on the OPTIONAL side (ml_uom): REJECT
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW idp_x WITH (incremental_refresh=true) AS
             SELECT ml_uom.id, count(*) c
             FROM idp_fct m JOIN idp_prod p ON p.id=m.prod_id
               JOIN idp_uom pt_uom ON pt_uom.id=p.tmpl_uom
               LEFT JOIN idp_ml ml ON ml.fact_id=m.id
               LEFT JOIN idp_uom ml_uom ON ml_uom.id=ml.ml_uom
             GROUP BY ml_uom.id';
    EXECUTE 'DROP MATERIALIZED VIEW idp_x';
  EXCEPTION WHEN feature_not_supported THEN ok := ok + 1; END;

  IF ok = 3 THEN RAISE NOTICE 'independent-dup accepted / real-self-join+outer rejected / optional-key rejected: PASS';
  ELSE RAISE EXCEPTION 'gate expectations failed, got %/3', ok; END IF;
END $$;

-- ---------------------------------------------------- differential (ml-only role)
\echo '--- per-role UNION correctness: an ml-only uom change == REFRESH (undercapture would miss it) ---'
CREATE MATERIALIZED VIEW idp_inc WITH (incremental_refresh=true) AS
  SELECT m.id, min(p.id) pid,
         sum(CASE WHEN ml.quantity IS NOT NULL THEN ml.quantity/ml_uom.factor*pt_uom.factor ELSE 0 END) c
  FROM idp_fct m JOIN idp_prod p ON p.id=m.prod_id
    JOIN idp_uom pt_uom ON pt_uom.id=p.tmpl_uom
    LEFT JOIN idp_ml ml ON ml.fact_id=m.id
    LEFT JOIN idp_uom ml_uom ON ml_uom.id=ml.ml_uom
  GROUP BY m.id;
CREATE MATERIALIZED VIEW idp_ref AS
  SELECT m.id, min(p.id) pid,
         sum(CASE WHEN ml.quantity IS NOT NULL THEN ml.quantity/ml_uom.factor*pt_uom.factor ELSE 0 END) c
  FROM idp_fct m JOIN idp_prod p ON p.id=m.prod_id
    JOIN idp_uom pt_uom ON pt_uom.id=p.tmpl_uom
    LEFT JOIN idp_ml ml ON ml.fact_id=m.id
    LEFT JOIN idp_uom ml_uom ON ml_uom.id=ml.ml_uom
  GROUP BY m.id;

DO $$
DECLARE d int;
BEGIN
  UPDATE idp_uom SET factor=8.0 WHERE id=3;   -- uom 3: reached ONLY via a move-line, no template
  UPDATE idp_uom SET factor=3.0 WHERE id=1;   -- uom 1: template of prod 10 (pt-only)
  UPDATE idp_uom SET factor=6.0 WHERE id=2;   -- uom 2: template AND move-line (shared)
  INSERT INTO idp_ml VALUES (1003,102,3,5.0); -- fan-out second line + reuse ml-only uom
  DELETE FROM idp_ml WHERE id=1002;
  DELETE FROM idp_fct WHERE id=101;
  REFRESH MATERIALIZED VIEW idp_ref;
  SELECT count(*) INTO d FROM (
    (SELECT id,pid,c FROM idp_inc EXCEPT SELECT id,pid,c FROM idp_ref)
    UNION ALL (SELECT id,pid,c FROM idp_ref EXCEPT SELECT id,pid,c FROM idp_inc)) z;
  IF d = 0 THEN RAISE NOTICE 'independent-duplicate (incl. ml-only role) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'independent-duplicate diverged from REFRESH by % row(s)', d; END IF;
END $$;

DROP TABLE idp_uom, idp_prod, idp_fct, idp_ml CASCADE;
\echo ''
