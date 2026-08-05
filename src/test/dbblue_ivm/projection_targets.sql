-- DBblue IVM — projection target columns (expressions over group keys and
-- aggregates), maintained by the recompute path.
--
-- A GROUP BY matview may now output not just bare group keys and bare
-- aggregates, but an immutable PROJECTION over them — e.g. COALESCE(SUM(a),0)+
-- SUM(b), SUM(x)/NULLIF(COUNT(*),0), CASE WHEN SUM(a)>0 THEN ... END, a||b,
-- jsonb ->> 'k'.  Such a view is forced onto the recompute engine (which
-- re-derives the whole target list verbatim from the live tables), so any
-- expression that is a DETERMINISTIC function of a group's keys and aggregate
-- values matches a full REFRESH exactly.  (The additive delta path can't
-- maintain a nonlinear expression, so incr_needs_recompute routes these to
-- recompute.)
--
-- Rejected, cleanly, at CREATE:
--   * a volatile/stable output expression (now(), CURRENT_DATE) — not a function
--     of the group; it would drift from REFRESH — put it in a view on top;
--   * a GROUP BY key that is not itself a SELECT output column (referenced only
--     inside an expression) — the engine keys the matview on the group columns,
--     so there must be a stored column for each key.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: projection target columns ==='
\echo ''

-- 1. Projections over aggregates + visible group keys, over an OUTER JOIN with
--    orphans; every delta kind, all == REFRESH.
DROP TABLE IF EXISTS pjt_f, pjt_d CASCADE;
CREATE TABLE pjt_d(id int primary key, code text, name text);
CREATE TABLE pjt_f(id int primary key, did int, a numeric NOT NULL, b numeric NOT NULL);
INSERT INTO pjt_d VALUES (1,'A','alpha'),(2,'B','beta'),(3,'C','gamma');
INSERT INTO pjt_f VALUES (1,1,10,4),(2,1,20,1),(3,2,30,9),(4,3,5,5),(5,NULL,7,7),(6,99,3,3);
CREATE MATERIALIZED VIEW pjt_i WITH (incremental_refresh=true) AS
  SELECT d.code, d.name, (d.code || '/' || d.name) label, NULL::int flag,
         COALESCE(SUM(f.a),0) + COALESCE(SUM(f.b),0) tot,
         SUM(f.a) / NULLIF(COUNT(*),0) avg_a,
         CASE WHEN SUM(f.a) > SUM(f.b) THEN 'a-wins' ELSE 'b-wins' END winner,
         COUNT(*) c
  FROM pjt_f f LEFT JOIN pjt_d d ON f.did=d.id
  GROUP BY d.code, d.name;
CREATE MATERIALIZED VIEW pjt_o AS
  SELECT d.code, d.name, (d.code || '/' || d.name) label, NULL::int flag,
         COALESCE(SUM(f.a),0) + COALESCE(SUM(f.b),0) tot,
         SUM(f.a) / NULLIF(COUNT(*),0) avg_a,
         CASE WHEN SUM(f.a) > SUM(f.b) THEN 'a-wins' ELSE 'b-wins' END winner,
         COUNT(*) c
  FROM pjt_f f LEFT JOIN pjt_d d ON f.did=d.id
  GROUP BY d.code, d.name;

INSERT INTO pjt_f VALUES (7,2,100,1);          -- into an existing group (tot/avg/winner recompute)
UPDATE pjt_f SET a=1 WHERE id=3;               -- flips winner for group B
UPDATE pjt_f SET did=3 WHERE id=1;             -- move a row between groups
DELETE FROM pjt_d WHERE id=1;                  -- orphan group A's rows -> NULL-keyed group
UPDATE pjt_d SET name='gamma2' WHERE id=3;     -- rename: label projection + group key change
INSERT INTO pjt_d VALUES (99,'Z','zeta');      -- de-orphan the did=99 row
DELETE FROM pjt_f WHERE id=4;                  -- delete
REFRESH MATERIALIZED VIEW pjt_o;
DO $$DECLARE nd int; BEGIN
  SELECT count(*) INTO nd FROM (
    (SELECT code,name,label,flag,tot,avg_a,winner,c FROM pjt_i
     EXCEPT SELECT code,name,label,flag,tot,avg_a,winner,c FROM pjt_o) UNION ALL
    (SELECT code,name,label,flag,tot,avg_a,winner,c FROM pjt_o
     EXCEPT SELECT code,name,label,flag,tot,avg_a,winner,c FROM pjt_i)) z;
  IF nd=0 THEN RAISE NOTICE 'projection over aggregates + group keys (outer join) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'projection targets: FAIL (% rows differ)', nd; END IF;
END$$;
DROP TABLE pjt_f, pjt_d CASCADE;

-- 2. Single-table projection (ratio + arithmetic over aggregates).
DROP TABLE IF EXISTS pjs CASCADE;
CREATE TABLE pjs(id int primary key, k int, a numeric NOT NULL, b numeric NOT NULL);
INSERT INTO pjs SELECT g, g%4, g, g*2 FROM generate_series(1,20) g;
CREATE MATERIALIZED VIEW pjs_i WITH (incremental_refresh=true) AS
  SELECT k, SUM(a)*2 - COALESCE(SUM(b),0) net, ROUND(AVG(a),2) ravg, COUNT(*) c
  FROM pjs GROUP BY k;
CREATE MATERIALIZED VIEW pjs_o AS
  SELECT k, SUM(a)*2 - COALESCE(SUM(b),0) net, ROUND(AVG(a),2) ravg, COUNT(*) c
  FROM pjs GROUP BY k;
INSERT INTO pjs VALUES (100,1,50,10);
UPDATE pjs SET a=999 WHERE id=5;
DELETE FROM pjs WHERE id=8;
REFRESH MATERIALIZED VIEW pjs_o;
DO $$DECLARE nd int; BEGIN
  SELECT count(*) INTO nd FROM (
    (SELECT k,net,ravg,c FROM pjs_i EXCEPT SELECT k,net,ravg,c FROM pjs_o) UNION ALL
    (SELECT k,net,ravg,c FROM pjs_o EXCEPT SELECT k,net,ravg,c FROM pjs_i)) z;
  IF nd=0 THEN RAISE NOTICE 'single-table projection (arith over aggregates) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'single-table projection: FAIL (% rows differ)', nd; END IF;
END$$;
DROP TABLE pjs CASCADE;

-- 3. Clean rejections (never accepted-and-wrong, never a crash).
\set ON_ERROR_STOP off
DROP TABLE IF EXISTS pjr_f, pjr_d CASCADE;
CREATE TABLE pjr_d(id int primary key, code text, name text);
CREATE TABLE pjr_f(id int primary key, did int, a numeric);
CREATE OR REPLACE FUNCTION _pjr(sql text) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'CREATE MATERIALIZED VIEW _pr WITH (incremental_refresh=true) AS '||sql||' WITH NO DATA';
  EXECUTE 'DROP MATERIALIZED VIEW _pr';
  RETURN true;                                    -- accepted
EXCEPTION WHEN feature_not_supported THEN RETURN false; END $$;   -- cleanly rejected
DO $d$
BEGIN
  IF _pjr($$SELECT did, CURRENT_DATE d, SUM(a) s FROM pjr_f GROUP BY did$$)
     THEN RAISE EXCEPTION 'CURRENT_DATE output: FAIL (accepted)';
     ELSE RAISE NOTICE 'CURRENT_DATE output (stable) rejected: PASS'; END IF;
  IF _pjr($$SELECT did, now() n, SUM(a) s FROM pjr_f GROUP BY did$$)
     THEN RAISE EXCEPTION 'now() output: FAIL (accepted)';
     ELSE RAISE NOTICE 'now() output (volatile) rejected: PASS'; END IF;
  IF _pjr($$SELECT (d.code||'-'||d.name) k, SUM(f.a) s FROM pjr_f f
            LEFT JOIN pjr_d d ON f.did=d.id GROUP BY d.code, d.name$$)
     THEN RAISE EXCEPTION 'junk group key: FAIL (accepted)';
     ELSE RAISE NOTICE 'GROUP BY key referenced only inside expression rejected: PASS'; END IF;
END$d$;
DROP FUNCTION _pjr(text);
DROP TABLE pjr_f, pjr_d CASCADE;

\echo ''
\echo '=== projection target columns test complete ==='
