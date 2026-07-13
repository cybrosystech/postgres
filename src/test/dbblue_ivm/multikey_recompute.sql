-- DBblue IVM — index-driven multi-key recompute (matview_incr.c
-- incr_append_recompute_tail + incr_inject_affected_filter present_only mode +
-- incr_key_never_null + the split vanished-delete).
--
-- A multi-key matview with NULLABLE / expression group keys (the real Odoo
-- report shape: a date bucket + inner fact measures + optional multi-hop
-- dimension attributes over LEFT joins) used to fall to a NULL-safe-but-
-- unindexed recompute that aggregated the ENTIRE live join on every delta, then
-- intersected it with the affected set via Nested Loops (both in _new_agg_ and
-- in the vanished-group DELETE) — O(all-groups x affected), slower than a full
-- REFRESH.  Now:
--   * _new_agg_ injects a per-key "k IN (SELECT col FROM _affected_)" so the
--     recompute of the all-non-NULL affected groups is index-driven;
--   * a generalized NULL arm recovers the partial-NULL (orphan) groups, its own
--     aggregate restricted on the keys proven never NULL (a NOT NULL column no
--     outer join can NULL-extend, or a STRICT expression over such — e.g. the
--     to_char() date bucket), and gated so it is skipped unless a NULL-keyed
--     group is actually affected;
--   * vanished groups are removed by a row-value "(k1,..,kn) IN (SELECT ..)" the
--     matview's unique index serves, with a NULL-safe _delnull_ CTE for the rare
--     NULL-keyed ones.
--
-- Correctness is what this file pins: the index-driven non-NULL arm, the
-- restricted NULL arm, and the split delete must all stay byte-identical to a
-- full REFRESH, across deltas that create/destroy orphan groups, move rows
-- between groups, and vanish groups — for both nullable-column and NOT NULL
-- always-present keys.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: index-driven multi-key recompute ==='
\echo ''

-- 1. Odoo invoice-KPI shape: to_char(date) bucket + NOT NULL price (fact) +
--    optional multi-hop template code/name + optional bunch_code.  The date is
--    NOT NULL, so the strict bucket expression is a "never NULL" key and the
--    NULL arm restricts on it; code/name/bunch are optional (LEFT) keys.
DROP TABLE IF EXISTS mkr_am, mkr_aml, mkr_pp, mkr_pt CASCADE;
CREATE TABLE mkr_pt(id int primary key, default_code text, name text);
CREATE TABLE mkr_pp(id int primary key, tmpl_id int, bunch_code text);
CREATE TABLE mkr_am(id int primary key, invoice_date date NOT NULL);
CREATE TABLE mkr_aml(id int primary key, move_id int NOT NULL, product_id int,
                     price_unit numeric NOT NULL, quantity numeric NOT NULL);
INSERT INTO mkr_pt VALUES (1,'A01','Alpha'),(2,'B02','Beta'),(3,'C03','Gamma');
INSERT INTO mkr_pp VALUES (10,1,'BN1'),(11,2,'BN2'),(12,NULL,'BN3'),(13,3,NULL);
INSERT INTO mkr_am VALUES (100,'2024-01-10'),(101,'2024-02-20'),(102,'2024-01-15');
INSERT INTO mkr_aml VALUES
  (1,100,10,50,5),(2,100,11,50,2),(3,101,12,20,1),(4,101,NULL,10,3),
  (5,102,13,50,4),(6,102,99,30,7);         -- aml 4: orphan product; aml 6: dangling product_id
CREATE MATERIALIZED VIEW mkr_i WITH (incremental_refresh=true) AS
  SELECT to_char(mkr_am.invoice_date,'YYYY-MM') mon, mkr_pt.default_code, mkr_pt.name,
         mkr_aml.price_unit, mkr_pp.bunch_code, sum(mkr_aml.quantity) qty, count(*) c
  FROM mkr_am JOIN mkr_aml ON mkr_aml.move_id=mkr_am.id
              LEFT JOIN mkr_pp ON mkr_aml.product_id=mkr_pp.id
              LEFT JOIN mkr_pt ON mkr_pp.tmpl_id=mkr_pt.id
  GROUP BY to_char(mkr_am.invoice_date,'YYYY-MM'), mkr_pt.default_code, mkr_pt.name,
           mkr_aml.price_unit, mkr_pp.bunch_code;
CREATE MATERIALIZED VIEW mkr_o AS
  SELECT to_char(mkr_am.invoice_date,'YYYY-MM') mon, mkr_pt.default_code, mkr_pt.name,
         mkr_aml.price_unit, mkr_pp.bunch_code, sum(mkr_aml.quantity) qty, count(*) c
  FROM mkr_am JOIN mkr_aml ON mkr_aml.move_id=mkr_am.id
              LEFT JOIN mkr_pp ON mkr_aml.product_id=mkr_pp.id
              LEFT JOIN mkr_pt ON mkr_pp.tmpl_id=mkr_pt.id
  GROUP BY to_char(mkr_am.invoice_date,'YYYY-MM'), mkr_pt.default_code, mkr_pt.name,
           mkr_aml.price_unit, mkr_pp.bunch_code;

DELETE FROM mkr_pt WHERE id=1;               -- orphans pp 10's lines (multi-hop) -> NULL-keyed groups
UPDATE mkr_pt SET name='Beta2' WHERE id=2;   -- deep rename: non-NULL groups move, NULL arm may fire on dragged-in orphans
UPDATE mkr_pp SET tmpl_id=3 WHERE id=12;     -- de-orphan aml 3 into template C
UPDATE mkr_pp SET bunch_code='BN2x' WHERE id=11;  -- mid non-join rename (bunch is a key)
INSERT INTO mkr_pt VALUES (4,'D04','Delta');
INSERT INTO mkr_pp VALUES (14,4,'BN4');
UPDATE mkr_aml SET product_id=14 WHERE id=6;      -- reconnect dangling line -> group vanishes/appears
INSERT INTO mkr_aml VALUES (7,100,10,50,9);       -- fact insert into an existing group
UPDATE mkr_aml SET price_unit=999 WHERE id=1;     -- fact key move (vanishes old (…,50,…) combo? recomputed)
UPDATE mkr_am SET invoice_date='2024-03-01' WHERE id=102;  -- bucket move (never-NULL key changes)
DELETE FROM mkr_aml WHERE id=5;                   -- fact delete
REFRESH MATERIALIZED VIEW mkr_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT mon,default_code,name,price_unit,bunch_code,qty,c FROM mkr_i
     EXCEPT SELECT mon,default_code,name,price_unit,bunch_code,qty,c FROM mkr_o)
    UNION ALL
    (SELECT mon,default_code,name,price_unit,bunch_code,qty,c FROM mkr_o
     EXCEPT SELECT mon,default_code,name,price_unit,bunch_code,qty,c FROM mkr_i)) z;
  IF d=0 THEN RAISE NOTICE 'multi-key nullable (date bucket + optional dims) recompute == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'multi-key recompute: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE mkr_am, mkr_aml, mkr_pp, mkr_pt CASCADE;

-- 2. All-optional keys (every group key from a LEFT-joined side, so NONE is
--    "never NULL"): the NULL arm cannot restrict and falls back to the full
--    aggregate — must still be correct.  Exercises the present_only "nothing
--    qualified" path and the _delnull_ NULL-keyed vanished delete.
DROP TABLE IF EXISTS ao_f, ao_d CASCADE;
CREATE TABLE ao_d(id int primary key, k1 text, k2 text);
CREATE TABLE ao_f(id int primary key, did int, amt numeric NOT NULL);
INSERT INTO ao_d VALUES (1,'x','p'),(2,'y','q'),(3,NULL,'r');
INSERT INTO ao_f VALUES (1,1,10),(2,1,20),(3,2,30),(4,3,40),(5,NULL,50),(6,7,60);
CREATE MATERIALIZED VIEW ao_i WITH (incremental_refresh=true) AS
  SELECT d.k1, d.k2, sum(f.amt) s, count(*) c
  FROM ao_f f LEFT JOIN ao_d d ON f.did=d.id GROUP BY d.k1, d.k2;
CREATE MATERIALIZED VIEW ao_o AS
  SELECT d.k1, d.k2, sum(f.amt) s, count(*) c
  FROM ao_f f LEFT JOIN ao_d d ON f.did=d.id GROUP BY d.k1, d.k2;
DELETE FROM ao_d WHERE id=1;                 -- vanishes ('x','p'), orphans f1,f2 into (NULL,NULL)
UPDATE ao_d SET k1='y2' WHERE id=2;          -- ('y','q') -> ('y2','q')
INSERT INTO ao_d VALUES (7,'z','s');         -- de-orphans f6 out of (NULL,NULL)
INSERT INTO ao_f VALUES (7,NULL,70);         -- more into the all-NULL group
REFRESH MATERIALIZED VIEW ao_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT k1,k2,s,c FROM ao_i EXCEPT SELECT k1,k2,s,c FROM ao_o) UNION ALL
    (SELECT k1,k2,s,c FROM ao_o EXCEPT SELECT k1,k2,s,c FROM ao_i)) z;
  IF d=0 THEN RAISE NOTICE 'all-optional multi-key (unrestricted NULL arm + _delnull_) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'all-optional multi-key: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ao_f, ao_d CASCADE;

\echo ''
\echo '=== multi-key recompute test complete ==='
