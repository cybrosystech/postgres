-- DBblue IVM — HAVING over expression-of-aggregate (COALESCE / CASE / arithmetic).
--
-- HAVING is maintained by a per-delta UPDATE that recomputes the hidden
-- __mv_having_ok__ visibility flag from the matview's STORED columns (the
-- incr_deparse_having_cond translator renders the condition over those columns).
-- Previously that translator handled only bare aggregate comparisons, so a
-- HAVING that WRAPPED an aggregate (COALESCE(SUM(a),0) > 0, a CASE over
-- aggregates, SUM(a)+SUM(b) > k) elog'd "unsupported expression type" at CREATE —
-- a gate/translator mismatch (the eligibility gate accepted it).
--
-- Now:
--   * the translator renders CoalesceExpr and (searched) CaseExpr;
--   * incr_having_expr_column binds a whole HAVING SUB-EXPRESSION to a stored
--     PROJECTION output column, so HAVING can reference a projection column
--     (SELECT COALESCE(SUM(a),0) AS x … HAVING COALESCE(SUM(a),0) > 0 → "x > 0")
--     — composing with projection target columns;
--   * a projection-column matview with HAVING uses the ruleutils backfill (the
--     hand backfill renderer can't render projection expressions).
-- A HAVING aggregate absent from the SELECT (nothing stored to evaluate against)
-- is still rejected cleanly.
--
-- Correctness is what this pins: groups crossing the HAVING boundary in BOTH
-- directions stay byte-identical to a full REFRESH.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: HAVING over expression-of-aggregate ==='
\echo ''

-- 1. COALESCE(SUM) > 0 HAVING (bare aggregate selected); boundary crossings both ways.
DROP TABLE IF EXISTS he CASCADE;
CREATE TABLE he(id int primary key, k int, a numeric NOT NULL);
INSERT INTO he VALUES (1,1,5),(2,1,3),(3,2,-10),(4,2,2),(5,3,0);
CREATE MATERIALIZED VIEW he_i WITH (incremental_refresh=true) AS
  SELECT k, SUM(a) s FROM he GROUP BY k HAVING COALESCE(SUM(a),0) > 0;
CREATE MATERIALIZED VIEW he_o AS
  SELECT k, SUM(a) s FROM he GROUP BY k HAVING COALESCE(SUM(a),0) > 0;
INSERT INTO he VALUES (6,2,20);       -- group 2: -8 -> 12, crosses IN
INSERT INTO he VALUES (7,1,-100);     -- group 1: 8 -> -92, crosses OUT
DELETE FROM he WHERE id=5;            -- group 3 stays failing
UPDATE he SET a=1 WHERE id=3;         -- group 2 shifts
REFRESH MATERIALIZED VIEW he_o;
DO $$DECLARE n int; BEGIN
  SELECT count(*) INTO n FROM ((SELECT k,s FROM he_i EXCEPT SELECT k,s FROM he_o) UNION ALL
                               (SELECT k,s FROM he_o EXCEPT SELECT k,s FROM he_i)) z;
  IF n=0 THEN RAISE NOTICE 'COALESCE(SUM) HAVING (bare agg) boundary-cross == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'COALESCE HAVING: FAIL (% differ)', n; END IF; END$$;
DROP TABLE he CASCADE;

-- 2. Projection column stored + HAVING references it (client pattern), over an
--    OUTER JOIN — exercises incr_having_expr_column binding + ruleutils backfill.
DROP TABLE IF EXISTS hf, hd CASCADE;
CREATE TABLE hd(id int primary key, code text);
CREATE TABLE hf(id int primary key, did int, a numeric NOT NULL, b numeric NOT NULL);
INSERT INTO hd VALUES (1,'A'),(2,'B'),(3,'C');
INSERT INTO hf VALUES (1,1,5,1),(2,1,3,2),(3,2,-10,4),(4,2,2,1),(5,3,0,0),(6,99,7,7);
CREATE MATERIALIZED VIEW hp_i WITH (incremental_refresh=true) AS
  SELECT d.code, COALESCE(SUM(f.a),0) x, SUM(f.a)+SUM(f.b) net, COUNT(*) c
  FROM hf f LEFT JOIN hd d ON f.did=d.id GROUP BY d.code
  HAVING COALESCE(SUM(f.a),0) > 0 OR CASE WHEN COUNT(*) >= 3 THEN true ELSE false END;
CREATE MATERIALIZED VIEW hp_o AS
  SELECT d.code, COALESCE(SUM(f.a),0) x, SUM(f.a)+SUM(f.b) net, COUNT(*) c
  FROM hf f LEFT JOIN hd d ON f.did=d.id GROUP BY d.code
  HAVING COALESCE(SUM(f.a),0) > 0 OR CASE WHEN COUNT(*) >= 3 THEN true ELSE false END;
INSERT INTO hf VALUES (7,2,50,1);     -- group B crosses IN (sum a)
UPDATE hf SET a=-999 WHERE id=1;      -- group A crosses OUT
DELETE FROM hd WHERE id=3;            -- orphan C's row -> NULL group
INSERT INTO hd VALUES (99,'Z');       -- de-orphan the did=99 row
DELETE FROM hf WHERE id=4;
REFRESH MATERIALIZED VIEW hp_o;
DO $$DECLARE n int; BEGIN
  SELECT count(*) INTO n FROM ((SELECT code,x,net,c FROM hp_i EXCEPT SELECT code,x,net,c FROM hp_o) UNION ALL
                               (SELECT code,x,net,c FROM hp_o EXCEPT SELECT code,x,net,c FROM hp_i)) z;
  IF n=0 THEN RAISE NOTICE 'projection + COALESCE/CASE HAVING (outer join) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'projection+HAVING: FAIL (% differ)', n; END IF; END$$;
DROP TABLE hf, hd CASCADE;

-- 3. Clean rejection: HAVING aggregate absent from the SELECT list (nothing
--    stored to evaluate the flag against) — refused, never elog/crash.
DROP TABLE IF EXISTS hr CASCADE;
CREATE TABLE hr(id int primary key, k int, a numeric, b numeric);
DO $d$
BEGIN
  BEGIN
    EXECUTE 'CREATE MATERIALIZED VIEW _h WITH (incremental_refresh=true) AS '
            'SELECT k, SUM(a) s FROM hr GROUP BY k HAVING SUM(b) > 0 WITH NO DATA';
    EXECUTE 'DROP MATERIALIZED VIEW _h';
    RAISE EXCEPTION 'HAVING aggregate absent from SELECT: FAIL (accepted)';
  EXCEPTION WHEN feature_not_supported THEN
    RAISE NOTICE 'HAVING aggregate absent from SELECT rejected: PASS';
  END;
END $d$;
DROP TABLE hr CASCADE;

\echo ''
\echo '=== HAVING expression-of-aggregate test complete ==='
