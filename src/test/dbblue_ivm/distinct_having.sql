-- DBblue IVM — recompute aggregates (COUNT(DISTINCT), stddev/variance/bool) WITH
-- a HAVING clause.
--
-- A recompute matview is stored as a hidden _dbblue_<oid>_base matview plus a
-- user-facing VIEW that filters on the __mv_having_ok__ flag; that flag is
-- recomputed after every delta by the separate hav_sql UPDATE, exactly as for
-- the additive shapes.  The recompute delta maintains EVERY group (passing or
-- failing); hav_sql then re-derives visibility.  This test exercises groups that
-- cross the HAVING boundary in BOTH directions, plus the WITH DATA failing-group
-- backfill (which must seed COUNT(DISTINCT ...) with its true value).  All
-- checked == a full REFRESH of an identically-defined plain matview.
--
-- NB: a HAVING incremental matview's original name is a VIEW, so tear down via
-- DROP TABLE ... CASCADE (which removes the base matview and the view).
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: DISTINCT / stddev / bool + HAVING ==='
\echo ''

-- 1. single-table COUNT(DISTINCT) + HAVING, boundary crossed up and down
DROP TABLE IF EXISTS hd CASCADE;
CREATE TABLE hd(id serial primary key, g int, v int);
CREATE MATERIALIZED VIEW hd_i WITH (incremental_refresh=true) AS
  SELECT g, count(DISTINCT v) dv, count(*) c FROM hd GROUP BY g HAVING count(DISTINCT v) >= 2;
CREATE MATERIALIZED VIEW hd_o AS
  SELECT g, count(DISTINCT v) dv, count(*) c FROM hd GROUP BY g HAVING count(DISTINCT v) >= 2;
INSERT INTO hd(g,v) VALUES (1,10),(1,10),(1,20),(2,5),(3,1),(3,2),(3,3);
DELETE FROM hd WHERE g=3 AND v=2;       -- g3 dv 3->2, stays visible
INSERT INTO hd(g,v) VALUES (2,6);        -- g2 dv 1->2, becomes visible
DELETE FROM hd WHERE g=1 AND v=20;       -- g1 dv 2->1, becomes hidden
REFRESH MATERIALIZED VIEW hd_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT g,dv,c FROM hd_i EXCEPT SELECT g,dv,c FROM hd_o)
    UNION ALL (SELECT g,dv,c FROM hd_o EXCEPT SELECT g,dv,c FROM hd_i)) z;
  IF d=0 THEN RAISE NOTICE 'single-table COUNT(DISTINCT)+HAVING, boundary both ways == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'single COUNT(DISTINCT)+HAVING: FAIL (% diff)', d; END IF; END$$;
DROP TABLE hd CASCADE;

-- 2. WITH DATA: most groups fail HAVING at create (failing-group backfill must
--    seed the TRUE distinct count); then one group crosses above the threshold.
DROP TABLE IF EXISTS hb CASCADE;
CREATE TABLE hb(id serial primary key, g int, v int);
INSERT INTO hb(g,v) SELECT i%5, i%3 FROM generate_series(1,60) i;   -- dv<=3 per group
CREATE MATERIALIZED VIEW hb_i WITH (incremental_refresh=true) AS
  SELECT g, count(DISTINCT v) dv, sum(v) s FROM hb GROUP BY g HAVING count(DISTINCT v) > 5;
INSERT INTO hb(g,v) SELECT 0, 100+i FROM generate_series(1,8) i;    -- g0 gains 8 distinct -> passes
CREATE MATERIALIZED VIEW hb_o AS
  SELECT g, count(DISTINCT v) dv, sum(v) s FROM hb GROUP BY g HAVING count(DISTINCT v) > 5;
REFRESH MATERIALIZED VIEW hb_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT g,dv,s FROM hb_i EXCEPT SELECT g,dv,s FROM hb_o)
    UNION ALL (SELECT g,dv,s FROM hb_o EXCEPT SELECT g,dv,s FROM hb_i)) z;
  IF d=0 THEN RAISE NOTICE 'WITH DATA failing-group backfill (true distinct) + cross-up == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'failing-group backfill: FAIL (% diff)', d; END IF; END$$;
DROP TABLE hb CASCADE;

-- 3. INNER JOIN COUNT(DISTINCT) + HAVING
DROP TABLE IF EXISTS jl, jd CASCADE;
CREATE TABLE jd(id int primary key, region text);
CREATE TABLE jl(id serial primary key, did int, amt int);
INSERT INTO jd VALUES (1,'E'),(2,'W'),(3,'N');
INSERT INTO jl(did,amt) VALUES (1,10),(1,10),(1,20),(2,5),(3,7),(3,7);
CREATE MATERIALIZED VIEW jhd_i WITH (incremental_refresh=true) AS
  SELECT d.region, count(DISTINCT l.amt) dv, count(*) c
  FROM jl l JOIN jd d ON d.id=l.did GROUP BY d.region HAVING count(DISTINCT l.amt) >= 2;
CREATE MATERIALIZED VIEW jhd_o AS
  SELECT d.region, count(DISTINCT l.amt) dv, count(*) c
  FROM jl l JOIN jd d ON d.id=l.did GROUP BY d.region HAVING count(DISTINCT l.amt) >= 2;
INSERT INTO jl(did,amt) VALUES (2,6),(3,8);   -- W,N cross up
DELETE FROM jl WHERE did=1 AND amt=20;         -- E crosses down
REFRESH MATERIALIZED VIEW jhd_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT region,dv,c FROM jhd_i EXCEPT SELECT region,dv,c FROM jhd_o)
    UNION ALL (SELECT region,dv,c FROM jhd_o EXCEPT SELECT region,dv,c FROM jhd_i)) z;
  IF d=0 THEN RAISE NOTICE 'INNER JOIN COUNT(DISTINCT)+HAVING, boundary both ways == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'JOIN COUNT(DISTINCT)+HAVING: FAIL (% diff)', d; END IF; END$$;
DROP TABLE jl, jd CASCADE;

-- 4. stddev + HAVING (boundary: 0/1/2+ rows produce different stddev)
DROP TABLE IF EXISTS sd CASCADE;
CREATE TABLE sd(id serial primary key, g int, v numeric);
INSERT INTO sd(g,v) VALUES (1,10),(1,20),(1,30),(2,5),(2,5),(3,1),(3,100);
CREATE MATERIALIZED VIEW sd_i WITH (incremental_refresh=true) AS
  SELECT g, stddev(v) sv, count(*) c FROM sd GROUP BY g HAVING stddev(v) > 5;
CREATE MATERIALIZED VIEW sd_o AS
  SELECT g, stddev(v) sv, count(*) c FROM sd GROUP BY g HAVING stddev(v) > 5;
INSERT INTO sd(g,v) VALUES (2,500);    -- g2 stddev 0 -> large, becomes visible
DELETE FROM sd WHERE g=3 AND v=100;    -- g3 -> 1 row -> stddev NULL, becomes hidden
REFRESH MATERIALIZED VIEW sd_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM ((SELECT g,sv,c FROM sd_i EXCEPT SELECT g,sv,c FROM sd_o)
    UNION ALL (SELECT g,sv,c FROM sd_o EXCEPT SELECT g,sv,c FROM sd_i)) z;
  IF d=0 THEN RAISE NOTICE 'stddev+HAVING boundary == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'stddev+HAVING: FAIL (% diff)', d; END IF; END$$;
DROP TABLE sd CASCADE;

\echo ''
\echo '=== DISTINCT / stddev / bool + HAVING test complete ==='
