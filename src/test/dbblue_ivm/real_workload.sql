-- DBblue IVM — real Odoo client KPI report topologies (RW cases).
--
-- Collected from a client Odoo DB.  Both KPI reports route a report dimension
-- through the variant/template split:
--       <line> LEFT JOIN product_product LEFT JOIN product_template
-- i.e. the GROUP BY key (product_template.default_code / .name) is a MULTI-HOP
-- optional key (two optional hops from the fact line).  This is the topology
-- the synthetic probe flagged, now confirmed as a common real pattern.
--
-- STATUS: multi-hop optional GROUP BY keys are REJECTED cleanly today (commit
-- 5869daa12fb closed the silent-wrong-results hole).  These cases assert that
-- rejection now; when general multi-hop orphan support lands they flip to
-- correctness checks (== REFRESH).  Simplified stand-in tables preserve the
-- join topology and optionality of the real schema.
\set ON_ERROR_STOP off
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

CREATE OR REPLACE FUNCTION _rw(sql text, label text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE ok bool := true;
BEGIN
  BEGIN EXECUTE sql; EXCEPTION
    WHEN feature_not_supported THEN ok := false;
    WHEN others THEN RAISE NOTICE '%: OTHER-ERROR % (%)', label, SQLSTATE, SQLERRM; RETURN;
  END;
  EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS _m CASCADE';
  -- multi-hop optional key: EXPECTED rejected today
  IF ok THEN RAISE EXCEPTION '%: FAIL (accepted — multi-hop must be rejected until supported)', label;
  ELSE RAISE NOTICE '%: rejected as expected (multi-hop optional key)', label; END IF;
END$$;

-- RW-01 Invoice KPI: account_move → account_move_line →? product_product →? product_template
--   GROUP BY product_template.default_code, .name  (multi-hop optional)
SELECT _rw($$
  CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS
    SELECT pt.default_code, pt.name, sum(aml.price) revenue, count(*) lines
    FROM am
    JOIN aml ON aml.move_id = am.id
    LEFT JOIN pp ON aml.product_id = pp.id
    LEFT JOIN pt ON pp.tmpl_id = pt.id
    LEFT JOIN rp ON am.partner_id = rp.id
    GROUP BY pt.default_code, pt.name
$$, 'RW-01 Invoice KPI (product_template multi-hop key)');

-- RW-02 Sales KPI: sale_order → sale_order_line →? product_product →? product_template,
--   plus DIRECT optional currency.  GROUP BY includes a direct optional key
--   (rc.symbol) AND multi-hop optional keys (pt.default_code, .name).
SELECT _rw($$
  CREATE MATERIALIZED VIEW _m WITH (incremental_refresh=true) AS
    SELECT rc.symbol, pt.default_code, pt.name, sum(sol.qty) qty, count(*) lines
    FROM so_
    JOIN sol ON sol.order_id = so_.id
    LEFT JOIN pp ON sol.product_id = pp.id
    LEFT JOIN pt ON pp.tmpl_id = pt.id
    LEFT JOIN rc ON so_.currency_id = rc.id
    LEFT JOIN rp ON so_.partner_id = rp.id
    GROUP BY rc.symbol, pt.default_code, pt.name
$$, 'RW-02 Sales KPI (currency direct + product_template multi-hop keys)');

DROP FUNCTION _rw(text, text);
DROP TABLE am, aml, so_, sol, pp, pt, rp, rc CASCADE;
\echo ''
\echo '=== real workload topology test complete ==='
