-- DBblue IVM — real Odoo client KPI report topologies (RW cases).
--
-- The two client KPI reports (invoice revenue, sales YTD), reproduced with their
-- FULL group-key sets on simplified stand-in tables.  Between them they exercise
-- every widening built for the Odoo workload:
--   • to_char(date,'mon'/'YYYY')   — STABLE expression / date-bucket key
--   • product_template.default_code/.name — MULTI-HOP optional key (line→pp→pt)
--   • product_product.bunch_code   — optional key off an inner-joined line
--   • account_move_line.price_unit — inner-dim key
--   • res_currency.symbol          — DIRECT optional key (RW-02)
-- Each is checked == a full REFRESH after DML across every table in the chain,
-- including deltas that orphan/de-orphan fact rows at each hop and move rows
-- between month buckets.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: real Odoo workload topologies (RW-01, RW-02) ==='
\echo ''

DROP TABLE IF EXISTS am, aml, so_, sol, pp, pt, rp, rc CASCADE;
CREATE TABLE pt(id int primary key, default_code text, name text);
CREATE TABLE pp(id int primary key, tmpl_id int, bunch_code text, default_code2 text);
CREATE TABLE rp(id int primary key, name text);
CREATE TABLE rc(id int primary key, symbol text);
CREATE TABLE am(id int primary key, partner_id int, invoice_date date);
CREATE TABLE aml(id int primary key, move_id int, product_id int, price_unit numeric, quantity numeric);
CREATE TABLE so_(id int primary key, partner_id int, currency_id int, date_order date);
CREATE TABLE sol(id int primary key, order_id int, product_id int, price_unit numeric, product_uom_qty numeric);

INSERT INTO pt VALUES (1,'A01','Alpha'),(2,'B02','Beta');
INSERT INTO pp VALUES (10,1,'BN1','D1'),(11,2,'BN2','D2'),(12,NULL,'BN3','D3');
INSERT INTO rp VALUES (100,'Acme'),(101,'Globex');
INSERT INTO rc VALUES (200,'USD'),(201,'EUR');
INSERT INTO am VALUES (1000,100,'2024-01-10'),(1001,101,'2024-02-20');
INSERT INTO aml VALUES (1,1000,10,50,5),(2,1000,11,30,2),(3,1001,12,20,1),(4,1001,NULL,10,3);
INSERT INTO so_ VALUES (2000,100,200,'2024-01-15'),(2001,101,201,'2024-03-05'),(2002,NULL,NULL,'2024-02-01');
INSERT INTO sol VALUES (1,2000,10,9,5),(2,2001,11,8,7),(3,2002,12,7,3),(4,2000,NULL,6,9);

-- RW-01 Invoice KPI (full group-key set)
CREATE MATERIALIZED VIEW rw01_i WITH (incremental_refresh=true, allow_stable_keys=true) AS
  SELECT to_char(am.invoice_date,'mon') mon, to_char(am.invoice_date,'YYYY') yr,
         pt.default_code, pt.name, aml.price_unit, pp.bunch_code,
         sum(aml.quantity) qty, count(pp.bunch_code) nb
  FROM am JOIN aml ON aml.move_id=am.id
          LEFT JOIN pp ON aml.product_id=pp.id
          LEFT JOIN pt ON pp.tmpl_id=pt.id
          LEFT JOIN rp ON am.partner_id=rp.id
  GROUP BY to_char(am.invoice_date,'mon'), to_char(am.invoice_date,'YYYY'),
           pt.default_code, pt.name, aml.price_unit, pp.bunch_code;
CREATE MATERIALIZED VIEW rw01_o AS
  SELECT to_char(am.invoice_date,'mon') mon, to_char(am.invoice_date,'YYYY') yr,
         pt.default_code, pt.name, aml.price_unit, pp.bunch_code,
         sum(aml.quantity) qty, count(pp.bunch_code) nb
  FROM am JOIN aml ON aml.move_id=am.id
          LEFT JOIN pp ON aml.product_id=pp.id
          LEFT JOIN pt ON pp.tmpl_id=pt.id
          LEFT JOIN rp ON am.partner_id=rp.id
  GROUP BY to_char(am.invoice_date,'mon'), to_char(am.invoice_date,'YYYY'),
           pt.default_code, pt.name, aml.price_unit, pp.bunch_code;

-- RW-02 Sales KPI (full group-key set: currency direct + product multi-hop)
CREATE MATERIALIZED VIEW rw02_i WITH (incremental_refresh=true, allow_stable_keys=true) AS
  SELECT to_char(so_.date_order,'Mon-YY') mon, pt.default_code, pt.name,
         sol.price_unit, pp.default_code2, pp.bunch_code, rc.symbol,
         sum(sol.product_uom_qty) qty, count(pp.bunch_code) nb
  FROM so_ JOIN sol ON sol.order_id=so_.id
           LEFT JOIN pp ON sol.product_id=pp.id
           LEFT JOIN pt ON pp.tmpl_id=pt.id
           LEFT JOIN rc ON so_.currency_id=rc.id
           LEFT JOIN rp ON so_.partner_id=rp.id
  GROUP BY to_char(so_.date_order,'Mon-YY'), pt.default_code, pt.name,
           sol.price_unit, pp.default_code2, pp.bunch_code, rc.symbol;
CREATE MATERIALIZED VIEW rw02_o AS
  SELECT to_char(so_.date_order,'Mon-YY') mon, pt.default_code, pt.name,
         sol.price_unit, pp.default_code2, pp.bunch_code, rc.symbol,
         sum(sol.product_uom_qty) qty, count(pp.bunch_code) nb
  FROM so_ JOIN sol ON sol.order_id=so_.id
           LEFT JOIN pp ON sol.product_id=pp.id
           LEFT JOIN pt ON pp.tmpl_id=pt.id
           LEFT JOIN rc ON so_.currency_id=rc.id
           LEFT JOIN rp ON so_.partner_id=rp.id
  GROUP BY to_char(so_.date_order,'Mon-YY'), pt.default_code, pt.name,
           sol.price_unit, pp.default_code2, pp.bunch_code, rc.symbol;

-- deltas across every hop + month-bucket moves + orphan births/deaths
DELETE FROM pt WHERE id=1;                       -- orphan template for pp 10
UPDATE pp SET tmpl_id=2 WHERE id=12;             -- re-template pp 12
INSERT INTO pt VALUES (3,'C03','Gamma');
UPDATE pp SET tmpl_id=3, bunch_code='BN2b' WHERE id=11;
DELETE FROM pp WHERE id=10;                      -- orphan at the product_product hop
INSERT INTO aml VALUES (5,1000,11,30,4);
INSERT INTO sol VALUES (5,2002,11,8,6);
UPDATE am SET invoice_date='2024-03-01' WHERE id=1000;   -- month-bucket move
UPDATE so_ SET currency_id=NULL WHERE id=2000;   -- orphan currency (direct)
DELETE FROM rc WHERE id=201;                     -- orphan currency for order 2001

REFRESH MATERIALIZED VIEW rw01_o;
REFRESH MATERIALIZED VIEW rw02_o;

DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT mon,yr,default_code,name,price_unit,bunch_code,qty,nb FROM rw01_i
     EXCEPT SELECT mon,yr,default_code,name,price_unit,bunch_code,qty,nb FROM rw01_o)
    UNION ALL
    (SELECT mon,yr,default_code,name,price_unit,bunch_code,qty,nb FROM rw01_o
     EXCEPT SELECT mon,yr,default_code,name,price_unit,bunch_code,qty,nb FROM rw01_i)) z;
  IF d=0 THEN RAISE NOTICE 'RW-01 Invoice KPI (month bucket + multi-hop template + bunch_code) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'RW-01: FAIL (% rows differ)', d; END IF;

  SELECT count(*) INTO d FROM (
    (SELECT mon,default_code,name,price_unit,default_code2,bunch_code,symbol,qty,nb FROM rw02_i
     EXCEPT SELECT mon,default_code,name,price_unit,default_code2,bunch_code,symbol,qty,nb FROM rw02_o)
    UNION ALL
    (SELECT mon,default_code,name,price_unit,default_code2,bunch_code,symbol,qty,nb FROM rw02_o
     EXCEPT SELECT mon,default_code,name,price_unit,default_code2,bunch_code,symbol,qty,nb FROM rw02_i)) z;
  IF d=0 THEN RAISE NOTICE 'RW-02 Sales KPI (month bucket + currency direct + product multi-hop) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'RW-02: FAIL (% rows differ)', d; END IF;
END$$;

DROP TABLE am, aml, so_, sol, pp, pt, rp, rc CASCADE;
\echo ''
\echo '=== real workload topology test complete ==='
