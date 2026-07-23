-- DBblue IVM — expression / date-bucket GROUP BY keys (the report month/year
-- bucket, e.g. GROUP BY to_char(invoice_date,'mon') or date_trunc('month', d)).
--
-- Expression keys are maintained by the recompute path (dbblue_deparse_query
-- renders the grouping expression verbatim and re-derives each affected group
-- from live).  A STABLE key (locale/timezone-dependent, e.g. to_char) is allowed
-- as well as IMMUTABLE: a touched group always matches a current REFRESH.  (A
-- STABLE key can drift from a full REFRESH for UNtouched groups only if lc_time/
-- TimeZone is later changed; a REFRESH re-syncs — documented caveat.)  VOLATILE
-- keys, and expression keys that reference an OPTIONAL join side, are rejected.
--
-- Every case is checked == a full REFRESH after INSERT/DELETE/UPDATE (including
-- UPDATEs that move a row to a different bucket and, over outer joins, deltas
-- that orphan fact rows).
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: expression / date-bucket GROUP BY keys ==='
\echo ''

-- 1. to_char month bucket over a multi-table OUTER join (the Odoo report shape:
--    fact JOIN line LEFT product_product LEFT product_template), bucket key on
--    the preserved fact side, alongside multi-hop optional product keys.
DROP TABLE IF EXISTS am, aml, pp, pt CASCADE;
CREATE TABLE pt(id int primary key, code text, name text);
CREATE TABLE pp(id int primary key, tmpl int);
CREATE TABLE am(id int primary key, d date);
CREATE TABLE aml(id int primary key, mid int, pid int, price numeric);
INSERT INTO pt VALUES (1,'A','Alpha'),(2,'B','Beta');
INSERT INTO pp VALUES (10,1),(11,2),(12,NULL);
INSERT INTO am VALUES (1,'2024-01-10'),(2,'2024-02-15');
INSERT INTO aml VALUES (1,1,10,100),(2,1,11,50),(3,2,12,20),(4,2,NULL,10);
CREATE MATERIALIZED VIEW dbk_i WITH (incremental_refresh=true, allow_stable_keys=true) AS
  SELECT to_char(am.d,'mon') m, pt.code, pt.name, sum(aml.price) rev, count(*) c
  FROM am JOIN aml ON aml.mid=am.id LEFT JOIN pp ON aml.pid=pp.id LEFT JOIN pt ON pp.tmpl=pt.id
  GROUP BY to_char(am.d,'mon'), pt.code, pt.name;
CREATE MATERIALIZED VIEW dbk_o AS
  SELECT to_char(am.d,'mon') m, pt.code, pt.name, sum(aml.price) rev, count(*) c
  FROM am JOIN aml ON aml.mid=am.id LEFT JOIN pp ON aml.pid=pp.id LEFT JOIN pt ON pp.tmpl=pt.id
  GROUP BY to_char(am.d,'mon'), pt.code, pt.name;
DELETE FROM pt WHERE id=1;                 -- orphan the template group
UPDATE pp SET tmpl=2 WHERE id=12;
INSERT INTO aml VALUES (5,2,10,15);
UPDATE am SET d='2024-03-01' WHERE id=1;   -- month bucket move
DELETE FROM pp WHERE id=11;                -- orphan at the product_product hop
REFRESH MATERIALIZED VIEW dbk_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT m,code,name,rev,c FROM dbk_i EXCEPT SELECT m,code,name,rev,c FROM dbk_o)
    UNION ALL (SELECT m,code,name,rev,c FROM dbk_o EXCEPT SELECT m,code,name,rev,c FROM dbk_i)) z;
  IF d=0 THEN RAISE NOTICE 'to_char month bucket + multi-hop keys over outer join == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'date bucket over outer join: FAIL (% differ)', d; END IF; END$$;
DROP TABLE am, aml, pp, pt CASCADE;

-- 2. single-table to_char + date_trunc buckets (stable + immutable), bucket move
DROP TABLE IF EXISTS ev CASCADE;
CREATE TABLE ev(id int primary key, d date, amt numeric);
INSERT INTO ev SELECT g, '2024-01-01'::date + (g*10), g FROM generate_series(1,20) g;
CREATE MATERIALIZED VIEW ev_i WITH (incremental_refresh=true, allow_stable_keys=true) AS
  SELECT to_char(d,'mon') m, date_trunc('month', d::timestamp) mt, sum(amt) s, count(*) c
  FROM ev GROUP BY to_char(d,'mon'), date_trunc('month', d::timestamp);
CREATE MATERIALIZED VIEW ev_o AS
  SELECT to_char(d,'mon') m, date_trunc('month', d::timestamp) mt, sum(amt) s, count(*) c
  FROM ev GROUP BY to_char(d,'mon'), date_trunc('month', d::timestamp);
INSERT INTO ev VALUES (100,'2024-01-03',99);
DELETE FROM ev WHERE id=5;
UPDATE ev SET d='2024-12-25' WHERE id=8;
REFRESH MATERIALIZED VIEW ev_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT m,mt,s,c FROM ev_i EXCEPT SELECT m,mt,s,c FROM ev_o)
    UNION ALL (SELECT m,mt,s,c FROM ev_o EXCEPT SELECT m,mt,s,c FROM ev_i)) z;
  IF d=0 THEN RAISE NOTICE 'single-table to_char + date_trunc buckets == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'single-table date bucket: FAIL (% differ)', d; END IF; END$$;
DROP TABLE ev CASCADE;

-- 3. rejections: VOLATILE key, and expression key over an OPTIONAL join side
\set ON_ERROR_STOP off
DROP TABLE IF EXISTS va, vb CASCADE;
CREATE TABLE va(id int primary key, k int);
CREATE TABLE vb(id int primary key, aid int, v int);
DO $$
DECLARE made bool;
BEGIN
  made:=false;
  BEGIN CREATE MATERIALIZED VIEW _v WITH (incremental_refresh=true, allow_stable_keys=true) AS
    SELECT (random()*10)::int r, count(*) c FROM va GROUP BY (random()*10)::int;
    made:=true; EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _v; RAISE EXCEPTION 'volatile key: FAIL (accepted)';
  ELSE RAISE NOTICE 'VOLATILE group key rejected: PASS'; END IF;

  made:=false;
  BEGIN CREATE MATERIALIZED VIEW _v WITH (incremental_refresh=true, allow_stable_keys=true) AS
    SELECT COALESCE(vb.v,-1) x, count(*) c FROM va LEFT JOIN vb ON vb.aid=va.id GROUP BY COALESCE(vb.v,-1);
    made:=true; EXCEPTION WHEN feature_not_supported THEN NULL; END;
  IF made THEN DROP MATERIALIZED VIEW _v; RAISE EXCEPTION 'optional-ref expr key: FAIL (accepted)';
  ELSE RAISE NOTICE 'expression key referencing OPTIONAL table rejected: PASS'; END IF;
END$$;
DROP TABLE va, vb CASCADE;
\echo ''
\echo '=== date-bucket / expression key test complete ==='
