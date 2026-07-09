-- DBblue IVM — real Odoo client KPI report topologies (RW cases).
--
-- Collected from a client Odoo DB.  Both KPI reports route a report dimension
-- through the variant/template split:
--       <line> LEFT JOIN product_product LEFT JOIN product_template
-- i.e. the GROUP BY key (product_template.default_code / .name) is a MULTI-HOP
-- optional key.  RW-02 additionally mixes a DIRECT optional key
-- (res_currency.symbol) with the multi-hop product keys.
--
-- SUPPORTED via the general orphan arm (re-projects the delta join with the
-- at-or-below-delta key columns NULLed).  Each case is checked == a full REFRESH
-- of an identically-defined plain matview after a mix of DML across every table
-- in the chain, including deltas that orphan/de-orphan fact rows at each hop.
-- Simplified stand-in tables preserve the join topology and optionality.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: real Odoo workload topologies (RW-01, RW-02) ==='
\echo ''

DROP TABLE IF EXISTS am, aml, so_, sol, pp, pt, rp, rc CASCADE;
CREATE TABLE pt(id int primary key, default_code text, name text);              -- product_template
CREATE TABLE pp(id int primary key, tmpl_id int);                                -- product_product → template
CREATE TABLE rp(id int primary key, name text);                                  -- res_partner
CREATE TABLE rc(id int primary key, symbol text);                                -- res_currency
CREATE TABLE am(id int primary key, partner_id int);                             -- account_move
CREATE TABLE aml(id int primary key, move_id int, product_id int, price numeric);-- account_move_line
CREATE TABLE so_(id int primary key, partner_id int, currency_id int);           -- sale_order
CREATE TABLE sol(id int primary key, order_id int, product_id int, qty numeric); -- sale_order_line

INSERT INTO pt VALUES (1,'A01','Alpha'),(2,'B02','Beta');
INSERT INTO pp VALUES (10,1),(11,2),(12,NULL);
INSERT INTO rp VALUES (100,'Acme'),(101,'Globex');
INSERT INTO rc VALUES (200,'USD'),(201,'EUR');
INSERT INTO am VALUES (1000,100),(1001,101);
INSERT INTO aml VALUES (1,1000,10,50),(2,1000,11,30),(3,1001,12,20),(4,1001,NULL,10);
INSERT INTO so_ VALUES (2000,100,200),(2001,101,201),(2002,NULL,NULL);
INSERT INTO sol VALUES (1,2000,10,5),(2,2001,11,7),(3,2002,12,3),(4,2000,NULL,9);

-- RW-01 Invoice KPI: GROUP BY product_template.default_code, .name (multi-hop)
CREATE MATERIALIZED VIEW rw01_i WITH (incremental_refresh=true) AS
  SELECT pt.default_code, pt.name, sum(aml.price) revenue, count(*) lines
  FROM am JOIN aml ON aml.move_id=am.id
          LEFT JOIN pp ON aml.product_id=pp.id
          LEFT JOIN pt ON pp.tmpl_id=pt.id
          LEFT JOIN rp ON am.partner_id=rp.id
  GROUP BY pt.default_code, pt.name;
CREATE MATERIALIZED VIEW rw01_o AS
  SELECT pt.default_code, pt.name, sum(aml.price) revenue, count(*) lines
  FROM am JOIN aml ON aml.move_id=am.id
          LEFT JOIN pp ON aml.product_id=pp.id
          LEFT JOIN pt ON pp.tmpl_id=pt.id
          LEFT JOIN rp ON am.partner_id=rp.id
  GROUP BY pt.default_code, pt.name;

-- RW-02 Sales KPI: GROUP BY currency.symbol (direct) + product_template (multi-hop)
CREATE MATERIALIZED VIEW rw02_i WITH (incremental_refresh=true) AS
  SELECT rc.symbol, pt.default_code, pt.name, sum(sol.qty) qty, count(*) lines
  FROM so_ JOIN sol ON sol.order_id=so_.id
           LEFT JOIN pp ON sol.product_id=pp.id
           LEFT JOIN pt ON pp.tmpl_id=pt.id
           LEFT JOIN rc ON so_.currency_id=rc.id
           LEFT JOIN rp ON so_.partner_id=rp.id
  GROUP BY rc.symbol, pt.default_code, pt.name;
CREATE MATERIALIZED VIEW rw02_o AS
  SELECT rc.symbol, pt.default_code, pt.name, sum(sol.qty) qty, count(*) lines
  FROM so_ JOIN sol ON sol.order_id=so_.id
           LEFT JOIN pp ON sol.product_id=pp.id
           LEFT JOIN pt ON pp.tmpl_id=pt.id
           LEFT JOIN rc ON so_.currency_id=rc.id
           LEFT JOIN rp ON so_.partner_id=rp.id
  GROUP BY rc.symbol, pt.default_code, pt.name;

-- deltas across every hop, incl. orphan births/deaths at each level
DELETE FROM pt WHERE id=1;                     -- orphans PP 10 → aml 1 / sol 1 lose template
UPDATE pp SET tmpl_id=2 WHERE id=12;           -- re-template PP 12
INSERT INTO pt VALUES (3,'C03','Gamma');
UPDATE pp SET tmpl_id=3 WHERE id=11;           -- move to new template
DELETE FROM pp WHERE id=10;                    -- orphans aml 1 / sol 1 at the PP hop
INSERT INTO aml VALUES (5,1000,11,15);
INSERT INTO sol VALUES (5,2002,11,4);
UPDATE so_ SET currency_id=NULL WHERE id=2000; -- orphan currency (direct) for its lines
DELETE FROM rc WHERE id=201;                   -- orphan currency for order 2001's lines

REFRESH MATERIALIZED VIEW rw01_o;
REFRESH MATERIALIZED VIEW rw02_o;

DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT default_code,name,revenue,lines FROM rw01_i EXCEPT SELECT default_code,name,revenue,lines FROM rw01_o)
    UNION ALL (SELECT default_code,name,revenue,lines FROM rw01_o EXCEPT SELECT default_code,name,revenue,lines FROM rw01_i)) z;
  IF d=0 THEN RAISE NOTICE 'RW-01 Invoice KPI (product_template multi-hop) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'RW-01: FAIL (% rows differ)', d; END IF;

  SELECT count(*) INTO d FROM (
    (SELECT symbol,default_code,name,qty,lines FROM rw02_i EXCEPT SELECT symbol,default_code,name,qty,lines FROM rw02_o)
    UNION ALL (SELECT symbol,default_code,name,qty,lines FROM rw02_o EXCEPT SELECT symbol,default_code,name,qty,lines FROM rw02_i)) z;
  IF d=0 THEN RAISE NOTICE 'RW-02 Sales KPI (currency direct + product multi-hop) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'RW-02: FAIL (% rows differ)', d; END IF;
END$$;

DROP TABLE am, aml, so_, sol, pp, pt, rp, rc CASCADE;
\echo ''
\echo '=== real workload topology test complete ==='
