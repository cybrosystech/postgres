-- DBblue IVM — coverage corpus classifier.
--
-- Answers "how many real reports can we maintain incrementally?" with a NUMBER
-- instead of "most of them".  For every candidate report view it attempts
--     CREATE MATERIALIZED VIEW … WITH (incremental_refresh=true) AS <select>
--                                                                 WITH NO DATA
-- and records the outcome:
--     ACCEPTED            — maintainable incrementally
--     REJECTED: <reason>  — cleanly refused at CREATE (feature_not_supported);
--                           the reason is the exact gate message, so rejections
--                           group into the concrete engine gaps that block them
--     ERROR: <msg>        — not a classification (missing table / bad SQL in the
--                           corpus itself)
-- The gate runs at DDL time, so WITH NO DATA classifies WITHOUT populating —
-- cheap enough to run against a live database.
--
-- ───────────────────────────────────────────────────────────────────────────
-- RUNNING AGAINST A REAL ODOO DATABASE (the point of this tool)
-- ───────────────────────────────────────────────────────────────────────────
-- Odoo report models (sale.report, account.invoice.report, pos.order.report, …)
-- are SQL views.  Load just the dbblue_classify() function below, then:
--
--   SELECT viewname,
--          dbblue_classify(pg_get_viewdef(('public.'||viewname)::regclass)) AS outcome
--   FROM pg_views
--   WHERE schemaname='public' AND viewname LIKE '%report%'
--   ORDER BY 2, 1;
--
--   -- one-line coverage number:
--   WITH r AS (
--     SELECT dbblue_classify(pg_get_viewdef(('public.'||viewname)::regclass)) o
--     FROM pg_views WHERE schemaname='public' AND viewname LIKE '%report%')
--   SELECT count(*) total,
--          count(*) FILTER (WHERE o='ACCEPTED') accepted,
--          round(100.0*count(*) FILTER (WHERE o='ACCEPTED')/nullif(count(*),0),1) pct
--   FROM r;
--
-- (Widen/narrow the LIKE to match your report views; pg_get_viewdef gives the
--  live definition, so this measures YOUR actual reports.)
--
-- The rest of this file is a STANDALONE seed corpus of representative Odoo
-- report shapes on stub tables, so the harness runs and produces a number now.
-- The seed % is illustrative of the SHAPE space, not any one client's reports —
-- run the live query above for the real number.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: coverage corpus classifier ==='
\echo ''

-- ── the classifier ──────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION dbblue_classify(v_sql text) RETURNS text
LANGUAGE plpgsql AS $fn$
DECLARE s text := regexp_replace(v_sql, ';\s*$', '');   -- pg_get_viewdef adds ';'
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _dbblue_probe WITH (incremental_refresh=true) AS '
          || s || ' WITH NO DATA';
  EXECUTE 'DROP MATERIALIZED VIEW _dbblue_probe';
  RETURN 'ACCEPTED';
EXCEPTION
  WHEN feature_not_supported THEN
    BEGIN EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS _dbblue_probe'; EXCEPTION WHEN others THEN NULL; END;
    RETURN 'REJECTED: ' || regexp_replace(SQLERRM, '^cannot use incremental_refresh: ', '');
  WHEN others THEN
    BEGIN EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS _dbblue_probe'; EXCEPTION WHEN others THEN NULL; END;
    RETURN 'ERROR: ' || SQLERRM;
END $fn$;

-- ── stub Odoo schema (only the columns the seed corpus touches) ──────────────
DROP SCHEMA IF EXISTS odoo CASCADE;
CREATE SCHEMA odoo;
SET search_path = odoo, public;

CREATE TABLE res_country(id int primary key, name text);
CREATE TABLE res_partner(id int primary key, name text, country_id int, company_id int);
CREATE TABLE res_currency(id int primary key, name text, symbol text);
CREATE TABLE res_currency_rate(id int primary key, currency_id int, name date, rate numeric);
CREATE TABLE product_category(id int primary key, name text, parent_id int);
CREATE TABLE product_template(id int primary key, name text, categ_id int, default_code text);
CREATE TABLE product_product(id int primary key, product_tmpl_id int, default_code text, barcode text);
CREATE TABLE account_journal(id int primary key, name text, type text);
CREATE TABLE account_move(id int primary key, partner_id int, company_id int, journal_id int,
                          invoice_date date NOT NULL, state text, move_type text, currency_id int);
CREATE TABLE account_move_line(id int primary key, move_id int NOT NULL, product_id int,
                               quantity numeric NOT NULL, price_subtotal numeric NOT NULL, balance numeric NOT NULL);
CREATE TABLE sale_order(id int primary key, partner_id int, company_id int, date_order date NOT NULL,
                        state text, currency_id int, user_id int);
CREATE TABLE sale_order_line(id int primary key, order_id int NOT NULL, product_id int,
                             product_uom_qty numeric NOT NULL, price_total numeric NOT NULL, price_subtotal numeric NOT NULL);
CREATE TABLE purchase_order(id int primary key, partner_id int, company_id int, date_order date NOT NULL, state text, currency_id int);
CREATE TABLE purchase_order_line(id int primary key, order_id int NOT NULL, product_id int, product_qty numeric NOT NULL, price_total numeric NOT NULL);
CREATE TABLE pos_session(id int primary key, config_id int, start_at date);
CREATE TABLE pos_order(id int primary key, partner_id int, company_id int, date_order date NOT NULL, state text, session_id int);
CREATE TABLE pos_order_line(id int primary key, order_id int NOT NULL, product_id int, qty numeric NOT NULL, price_subtotal numeric NOT NULL);
CREATE TABLE stock_location(id int primary key, name text, usage text);
CREATE TABLE stock_move(id int primary key, product_id int, date date NOT NULL, location_id int, location_dest_id int,
                        product_qty numeric NOT NULL, state text, company_id int);
CREATE TABLE crm_lead(id int primary key, user_id int, team_id int, stage_id int,
                      expected_revenue numeric, probability numeric, create_date date, company_id int);

-- ── the seed corpus: (module, report, expected, sql) ─────────────────────────
-- `expected` is my prediction; the run flags any surprise (expected≠actual),
-- which doubles as a regression check on the accept/reject boundary.
CREATE TEMP TABLE _corpus(module text, report text, expected text, sql text);
INSERT INTO _corpus VALUES

-- ---- expected ACCEPTED: the bread-and-butter aggregate report shapes ----
('sale','sale.report (order line × order × product, grouped)','ACCEPTED', $$
  SELECT s.date_order, s.partner_id, s.company_id, s.state, t.categ_id,
         sum(l.product_uom_qty) qty, sum(l.price_total) total, count(*) nbr
  FROM sale_order_line l JOIN sale_order s ON l.order_id=s.id
       LEFT JOIN product_product p ON l.product_id=p.id
       LEFT JOIN product_template t ON p.product_tmpl_id=t.id
  GROUP BY s.date_order, s.partner_id, s.company_id, s.state, t.categ_id $$),

('purchase','purchase.report','ACCEPTED', $$
  SELECT p.date_order, p.partner_id, p.company_id, p.state,
         sum(l.product_qty) qty, sum(l.price_total) total, count(*) nbr
  FROM purchase_order_line l JOIN purchase_order p ON l.order_id=p.id
  GROUP BY p.date_order, p.partner_id, p.company_id, p.state $$),

('account','account.invoice.report (month bucket, multi-hop product)','ACCEPTED', $$
  SELECT to_char(m.invoice_date,'YYYY-MM') mon, m.move_type, m.partner_id,
         t.categ_id, t.default_code,
         sum(l.price_subtotal) amt, sum(l.quantity) qty, count(*) nbr
  FROM account_move_line l JOIN account_move m ON l.move_id=m.id
       LEFT JOIN product_product pp ON l.product_id=pp.id
       LEFT JOIN product_template t ON pp.product_tmpl_id=t.id
  GROUP BY to_char(m.invoice_date,'YYYY-MM'), m.move_type, m.partner_id, t.categ_id, t.default_code $$),

('pos','pos.order.report (line × order × session)','ACCEPTED', $$
  SELECT o.date_order, o.company_id, sess.config_id,
         sum(l.qty) qty, sum(l.price_subtotal) amt, count(*) nbr
  FROM pos_order_line l JOIN pos_order o ON l.order_id=o.id
       LEFT JOIN pos_session sess ON o.session_id=sess.id
  GROUP BY o.date_order, o.company_id, sess.config_id $$),

('stock','stock move by product/location','ACCEPTED', $$
  SELECT sm.product_id, sm.location_id, sm.location_dest_id, sm.state,
         sum(sm.product_qty) qty, count(*) nbr
  FROM stock_move sm GROUP BY sm.product_id, sm.location_id, sm.location_dest_id, sm.state $$),

('account','invoice by partner country (multi-hop optional)','ACCEPTED', $$
  SELECT c.name country, m.move_type, sum(l.price_subtotal) amt, count(*) nbr
  FROM account_move_line l JOIN account_move m ON l.move_id=m.id
       LEFT JOIN res_partner rp ON m.partner_id=rp.id
       LEFT JOIN res_country c ON rp.country_id=c.id
  GROUP BY c.name, m.move_type $$),

('sale','sale with currency (direct optional dim)','ACCEPTED', $$
  SELECT cur.symbol, s.state, sum(l.price_total) total, count(*) nbr
  FROM sale_order_line l JOIN sale_order s ON l.order_id=s.id
       LEFT JOIN res_currency cur ON s.currency_id=cur.id
  GROUP BY cur.symbol, s.state $$),

('sale','sale avg/min/max per product','ACCEPTED', $$
  SELECT l.product_id, avg(l.price_subtotal) a, min(l.price_subtotal) mn,
         max(l.price_subtotal) mx, count(*) nbr
  FROM sale_order_line l GROUP BY l.product_id $$),

('crm','crm pipeline by user/stage','ACCEPTED', $$
  SELECT user_id, team_id, stage_id, company_id,
         sum(expected_revenue) rev, avg(probability) prob, count(*) nbr
  FROM crm_lead GROUP BY user_id, team_id, stage_id, company_id $$),

('account','distinct partners per journal','ACCEPTED', $$
  SELECT m.journal_id, count(DISTINCT m.partner_id) partners, count(*) nbr
  FROM account_move m GROUP BY m.journal_id $$),

('sale','confirmed-only totals (FILTER)','ACCEPTED', $$
  SELECT s.company_id,
         sum(l.price_total) FILTER (WHERE s.state='sale') confirmed_total,
         count(*) FILTER (WHERE s.state='sale') confirmed_nbr
  FROM sale_order_line l JOIN sale_order s ON l.order_id=s.id
  GROUP BY s.company_id $$),

('product','product codes per category (string_agg)','ACCEPTED', $$
  SELECT t.categ_id, string_agg(t.default_code, ',') codes, count(*) nbr
  FROM product_template t GROUP BY t.categ_id $$),

('sale','sales by month bucket (date_trunc)','ACCEPTED', $$
  SELECT date_trunc('month', s.date_order::timestamp) m, s.company_id,
         sum(l.price_total) total
  FROM sale_order_line l JOIN sale_order s ON l.order_id=s.id
  GROUP BY date_trunc('month', s.date_order::timestamp), s.company_id $$),

-- ---- expected REJECTED: the concrete engine gaps ----
('account','account.invoice.report FAITHFUL (currency-rate correlated subquery)','REJECTED', $$
  SELECT m.invoice_date, m.partner_id,
         sum(l.balance / (SELECT r.rate FROM res_currency_rate r
                          WHERE r.currency_id=m.currency_id AND r.name<=m.invoice_date
                          ORDER BY r.name DESC LIMIT 1)) amt_company_cur
  FROM account_move_line l JOIN account_move m ON l.move_id=m.id
  GROUP BY m.invoice_date, m.partner_id $$),

('stock','stock running balance (window sum OVER)','REJECTED', $$
  SELECT sm.product_id, sm.date,
         sum(sm.product_qty) OVER (PARTITION BY sm.product_id ORDER BY sm.date) running_qty
  FROM stock_move sm $$),

('account','invoice + refund combined (UNION ALL)','REJECTED', $$
  SELECT m.partner_id, sum(l.price_subtotal) amt FROM account_move_line l
    JOIN account_move m ON l.move_id=m.id WHERE m.move_type='out_invoice' GROUP BY m.partner_id
  UNION ALL
  SELECT m.partner_id, -sum(l.price_subtotal) amt FROM account_move_line l
    JOIN account_move m ON l.move_id=m.id WHERE m.move_type='out_refund' GROUP BY m.partner_id $$),

('stock','latest price per product (DISTINCT ON)','REJECTED', $$
  SELECT DISTINCT ON (l.product_id) l.product_id, l.price_subtotal, m.invoice_date
  FROM account_move_line l JOIN account_move m ON l.move_id=m.id
  ORDER BY l.product_id, m.invoice_date DESC $$),

('sale','sales ranked within company (rank window)','REJECTED', $$
  SELECT s.company_id, l.product_id, sum(l.price_total) total,
         rank() OVER (PARTITION BY s.company_id ORDER BY sum(l.price_total) DESC) rnk
  FROM sale_order_line l JOIN sale_order s ON l.order_id=s.id
  GROUP BY s.company_id, l.product_id $$),

('account','report via CTE','REJECTED', $$
  WITH lines AS (SELECT move_id, sum(price_subtotal) amt FROM account_move_line GROUP BY move_id)
  SELECT m.partner_id, sum(lines.amt) total
  FROM lines JOIN account_move m ON lines.move_id=m.id
  GROUP BY m.partner_id $$),

('sale','per-order latest line via LATERAL','REJECTED', $$
  SELECT s.id, top.price_total
  FROM sale_order s
       JOIN LATERAL (SELECT l.price_total FROM sale_order_line l
                     WHERE l.order_id=s.id ORDER BY l.price_total DESC LIMIT 1) top ON true $$);

-- ── run + report ────────────────────────────────────────────────────────────
CREATE TEMP TABLE _res AS
  SELECT module, report, expected, dbblue_classify(sql) AS actual FROM _corpus;

\echo '--- per-report outcome ---'
SELECT module, report,
       CASE WHEN actual='ACCEPTED' THEN 'ACCEPTED'
            WHEN actual LIKE 'REJECTED%' THEN actual
            ELSE actual END AS outcome,
       CASE WHEN actual LIKE expected||'%' THEN '' ELSE '  <-- SURPRISE (expected '||expected||')' END AS note
FROM _res ORDER BY (actual='ACCEPTED') DESC, module, report;

\echo ''
\echo '--- coverage summary ---'
SELECT count(*) total,
       count(*) FILTER (WHERE actual='ACCEPTED') accepted,
       round(100.0*count(*) FILTER (WHERE actual='ACCEPTED')/nullif(count(*),0),1) accepted_pct,
       count(*) FILTER (WHERE actual LIKE 'REJECTED%') rejected,
       count(*) FILTER (WHERE actual LIKE 'ERROR%') errors
FROM _res;

\echo ''
\echo '--- rejections grouped by reason (the concrete engine gaps) ---'
SELECT regexp_replace(actual, '^REJECTED: ', '') AS reason, count(*)
FROM _res WHERE actual LIKE 'REJECTED%'
GROUP BY 1 ORDER BY 2 DESC, 1;

\echo ''
\echo '--- surprises (gate disagreed with expectation) ---'
SELECT module, report, expected, actual
FROM _res WHERE actual NOT LIKE expected||'%' ORDER BY module, report;

RESET search_path;
DROP SCHEMA odoo CASCADE;
DROP FUNCTION dbblue_classify(text);
\echo ''
\echo '=== corpus classifier complete ==='
