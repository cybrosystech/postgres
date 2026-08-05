-- DBblue IVM — self-join: a group whose key VANISHES must be removed from the
-- matview (regression for a bug found by the NULL-key adversarial audit).
--
-- The self-join recompute path collects "affected" group keys by joining the
-- delta to the LIVE table in each role (delta⋈live, twice).  When a whole
-- join-key partition is removed in one statement, the rows that formed a group
-- are ALL in the delta, so neither delta⋈live arm produces that group's key —
-- it never reaches the DELETE-vanished reconciliation and a STALE matview row
-- survived forever.  The fix adds a delta⋈delta arm (DELETE only) so the
-- vanished key is recovered.  The defect was NOT NULL-specific: a purely
-- value-keyed group vanished just as wrongly; NULL just made it visible once
-- NULL group keys started being maintained.
--
-- Every case is checked == a full REFRESH (the oracle) AND the NULL/empty group
-- is confirmed actually gone, so the test can't pass vacuously.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: self-join vanishing-group regression ==='
\echo ''

-- 1. VALUE-keyed vanish: delete an entire join-key partition; its group goes away.
DROP TABLE IF EXISTS sjv CASCADE;
CREATE TABLE sjv(id int primary key, k int, g int, v int);
INSERT INTO sjv VALUES (5,2,7,5),(6,2,7,6),(3,7,100,30);
CREATE MATERIALIZED VIEW sjv_i WITH (incremental_refresh=true) AS
  SELECT a.g, count(*) c, sum(a.v) sv FROM sjv a JOIN sjv b ON a.k=b.k GROUP BY a.g;
CREATE MATERIALIZED VIEW sjv_o AS
  SELECT a.g, count(*) c, sum(a.v) sv FROM sjv a JOIN sjv b ON a.k=b.k GROUP BY a.g;
DELETE FROM sjv WHERE k=2;                  -- group g=7 vanishes entirely
REFRESH MATERIALIZED VIEW sjv_o;
DO $$
DECLARE d int; stale int;
BEGIN
  SELECT count(*) INTO stale FROM sjv_i WHERE g=7;
  SELECT count(*) INTO d FROM ((SELECT g,c,sv FROM sjv_i EXCEPT SELECT g,c,sv FROM sjv_o)
    UNION ALL (SELECT g,c,sv FROM sjv_o EXCEPT SELECT g,c,sv FROM sjv_i)) z;
  IF d=0 AND stale=0 THEN RAISE NOTICE 'value-keyed vanish removed + == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'value-keyed vanish: FAIL (diff=%, stale g=7 rows=%)', d, stale; END IF;
END $$;
DROP MATERIALIZED VIEW sjv_i; DROP MATERIALIZED VIEW sjv_o; DROP TABLE sjv CASCADE;

-- 2. NULL-keyed vanish: same shape, the vanishing group has a NULL key.
DROP TABLE IF EXISTS sjn CASCADE;
CREATE TABLE sjn(id int primary key, k int, g int, v int);
INSERT INTO sjn VALUES (5,2,NULL,5),(6,2,NULL,6),(3,7,100,30);
CREATE MATERIALIZED VIEW sjn_i WITH (incremental_refresh=true) AS
  SELECT a.g, count(*) c, sum(a.v) sv FROM sjn a JOIN sjn b ON a.k=b.k GROUP BY a.g;
CREATE MATERIALIZED VIEW sjn_o AS
  SELECT a.g, count(*) c, sum(a.v) sv FROM sjn a JOIN sjn b ON a.k=b.k GROUP BY a.g;
DELETE FROM sjn WHERE id IN (5,6);          -- NULL group vanishes
REFRESH MATERIALIZED VIEW sjn_o;
DO $$
DECLARE d int; stale int;
BEGIN
  SELECT count(*) INTO stale FROM sjn_i WHERE g IS NULL;
  SELECT count(*) INTO d FROM ((SELECT g,c,sv FROM sjn_i EXCEPT SELECT g,c,sv FROM sjn_o)
    UNION ALL (SELECT g,c,sv FROM sjn_o EXCEPT SELECT g,c,sv FROM sjn_i)) z;
  IF d=0 AND stale=0 THEN RAISE NOTICE 'NULL-keyed vanish removed + == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'NULL-keyed vanish: FAIL (diff=%, stale NULL rows=%)', d, stale; END IF;
END $$;
DROP MATERIALIZED VIEW sjn_i; DROP MATERIALIZED VIEW sjn_o; DROP TABLE sjn CASCADE;

-- 3. COMPOSITE key spanning BOTH self-join sides; UPDATE flips the key value
--    (join key unchanged) so the OLD composite group must vanish, and a DELETE
--    that empties the other partition.
DROP TABLE IF EXISTS sjc CASCADE;
CREATE TABLE sjc(id int primary key, k int, ga int, gb int, v int);
INSERT INTO sjc VALUES (1,1,NULL,NULL,10),(2,2,5,5,20);
CREATE MATERIALIZED VIEW sjc_i WITH (incremental_refresh=true) AS
  SELECT t1.ga, t2.gb, count(*) cnt, sum(t1.v) sv
  FROM sjc t1 JOIN sjc t2 ON t1.k=t2.k GROUP BY t1.ga, t2.gb;
CREATE MATERIALIZED VIEW sjc_o AS
  SELECT t1.ga, t2.gb, count(*) cnt, sum(t1.v) sv
  FROM sjc t1 JOIN sjc t2 ON t1.k=t2.k GROUP BY t1.ga, t2.gb;
UPDATE sjc SET ga=3, gb=3 WHERE k=1;        -- (NULL,NULL) group must vanish, (3,3) appears
DELETE FROM sjc WHERE k=2;                   -- (5,5) group must vanish
REFRESH MATERIALIZED VIEW sjc_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM ((SELECT ga,gb,cnt,sv FROM sjc_i EXCEPT SELECT ga,gb,cnt,sv FROM sjc_o)
    UNION ALL (SELECT ga,gb,cnt,sv FROM sjc_o EXCEPT SELECT ga,gb,cnt,sv FROM sjc_i)) z;
  IF d=0 THEN RAISE NOTICE 'composite key-flip + partition-delete vanish == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'composite vanish: FAIL (% diff)', d; END IF;
END $$;
DROP MATERIALIZED VIEW sjc_i; DROP MATERIALIZED VIEW sjc_o; DROP TABLE sjc CASCADE;

-- 4. Randomized lifecycle: many inserts/deletes/key-flips, including full
--    partition removals, checked == REFRESH at the end.
DROP TABLE IF EXISTS sjr CASCADE;
CREATE TABLE sjr(id serial primary key, k int, g int, v numeric);
INSERT INTO sjr(k,g,v)
  SELECT (i%6), CASE WHEN i%5=0 THEN NULL ELSE i%4 END, (i%50)+1
  FROM generate_series(1,120) i;
CREATE MATERIALIZED VIEW sjr_i WITH (incremental_refresh=true) AS
  SELECT a.g, count(*) c, sum(a.v) sv, avg(a.v) av
  FROM sjr a JOIN sjr b ON a.k=b.k GROUP BY a.g;
CREATE MATERIALIZED VIEW sjr_o AS
  SELECT a.g, count(*) c, sum(a.v) sv, avg(a.v) av
  FROM sjr a JOIN sjr b ON a.k=b.k GROUP BY a.g;
DELETE FROM sjr WHERE k=0;                                -- empty a whole partition
UPDATE sjr SET g = CASE WHEN g IS NULL THEN 9 ELSE NULL END WHERE k=1;  -- flip keys to/from NULL
INSERT INTO sjr(k,g,v) VALUES (7,NULL,11),(7,NULL,12),(8,3,13);
DELETE FROM sjr WHERE k=2;                                -- empty another partition
UPDATE sjr SET k=99 WHERE k=3;                            -- move a partition (group preserved)
REFRESH MATERIALIZED VIEW sjr_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM ((SELECT g,c,sv,av FROM sjr_i EXCEPT SELECT g,c,sv,av FROM sjr_o)
    UNION ALL (SELECT g,c,sv,av FROM sjr_o EXCEPT SELECT g,c,sv,av FROM sjr_i)) z;
  IF d=0 THEN RAISE NOTICE 'randomized self-join lifecycle (incl. vanishing groups) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'randomized self-join lifecycle: FAIL (% diff)', d; END IF;
END $$;
DROP MATERIALIZED VIEW sjr_i; DROP MATERIALIZED VIEW sjr_o; DROP TABLE sjr CASCADE;

\echo ''
\echo '=== self-join vanishing-group regression complete ==='
