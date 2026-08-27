-- DBblue IVM — NULL-transition gate on the general orphan arm (matview_incr.c
-- incr_collect_delta_join_cols + the "join columns unchanged" EXCEPT ALL guard,
-- paired with the empty complementary ENR registered in matview_delta_apply).
--
-- The orphan arm exists to catch fact rows whose orphan status FLIPS because of
-- a delta on a key-contributing (non-anchor) table.  A delta that changed no
-- JOIN column of that table — e.g. a plain rename of a dimension's label — can
-- flip nobody's reachability, yet used to emit a NULL group key anyway, forcing
-- the unrestricted O(fact) NULL arm in _new_agg_ to recompute the whole NULL
-- group for nothing (measured: a rename on a 500k-fact deep-dimension matview
-- cost ~290ms — two full-fact scans — vs ~15ms now).
--
-- The gate guards the orphan arm with EXISTS over the symmetric multiset
-- difference of the delta table's join columns between its two transition
-- images; empty (⟺ every join-column tuple unchanged ⟺ no orphan transition
-- possible) suppresses the arm, so no NULL reaches _affected_ and the NULL arm's
-- uncorrelated EXISTS gate skips its scan.  A pure INSERT/DELETE has one image
-- empty (the complementary ENR), so the difference is non-empty and the arm
-- always fires.  Non-clean join quals (OR / non-equality / expression) decline
-- the gate and keep the unconditional, always-correct arm.
--
-- Correctness is what this file asserts: every delta kind — the ones the gate
-- SUPPRESSES (renames) and the ones it FIRES (real orphan/de-orphan flips) —
-- must stay byte-identical to a full REFRESH.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: NULL-transition gate (orphan-arm firing precision) ==='
\echo ''

-- 1. Multi-hop chain (fact -> prod -> tmpl), group key on the deep table.
--    Interleave renames (gate suppresses) with real transitions (gate fires):
--    each must equal a full REFRESH.
DROP TABLE IF EXISTS ntg_f, ntg_p, ntg_t CASCADE;
CREATE TABLE ntg_t(id int primary key, code text);
CREATE TABLE ntg_p(id int primary key, tmpl int, bunch text);
CREATE TABLE ntg_f(id int primary key, pid int, amt numeric);
INSERT INTO ntg_t VALUES (1,'A'),(2,'B'),(3,'C');
INSERT INTO ntg_p VALUES (10,1,'x'),(11,2,'y'),(12,NULL,'z');
INSERT INTO ntg_f VALUES (1,10,5),(2,10,7),(3,11,3),(4,12,9),(5,NULL,1),(6,99,2);
CREATE MATERIALIZED VIEW ntg_i WITH (incremental_refresh=true) AS
  SELECT t.code gk, sum(f.amt) s, count(*) c, min(f.amt) mn, max(f.amt) mx
  FROM ntg_f f LEFT JOIN ntg_p p ON f.pid=p.id LEFT JOIN ntg_t t ON p.tmpl=t.id
  GROUP BY t.code;
CREATE MATERIALIZED VIEW ntg_o AS
  SELECT t.code gk, sum(f.amt) s, count(*) c, min(f.amt) mn, max(f.amt) mx
  FROM ntg_f f LEFT JOIN ntg_p p ON f.pid=p.id LEFT JOIN ntg_t t ON p.tmpl=t.id
  GROUP BY t.code;

UPDATE ntg_t SET code='A2' WHERE id=1;          -- deep rename: gate SUPPRESSES
UPDATE ntg_p SET bunch='q' WHERE id=10;         -- middle non-join rename: SUPPRESSES
UPDATE ntg_t SET code=code||'!' WHERE id BETWEEN 1 AND 3;  -- multi-row rename: SUPPRESSES
DELETE FROM ntg_t WHERE id=2;                   -- real orphan (ntg_p 11 -> NULL): FIRES
UPDATE ntg_p SET tmpl=3 WHERE id=12;            -- real de-orphan (ntg_f 4 -> C): FIRES
UPDATE ntg_p SET tmpl=NULL WHERE id=10;         -- real orphan via fk->NULL: FIRES
INSERT INTO ntg_t VALUES (4,'D');               -- dim insert
INSERT INTO ntg_p VALUES (13,4,'w');            -- reconnect chain
UPDATE ntg_f SET pid=13 WHERE id=6;             -- de-orphan fact (99 was dangling): FIRES
REFRESH MATERIALIZED VIEW ntg_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,s,c,mn,mx FROM ntg_i EXCEPT SELECT gk,s,c,mn,mx FROM ntg_o) UNION ALL
    (SELECT gk,s,c,mn,mx FROM ntg_o EXCEPT SELECT gk,s,c,mn,mx FROM ntg_i)) z;
  IF d=0 THEN RAISE NOTICE 'renames (suppressed) + real transitions (fired) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'NULL-transition gate multi-hop: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ntg_f, ntg_p, ntg_t CASCADE;

-- 2. Direct optional dimension: a rename of the dimension's key column itself
--    (the GROUP BY key) is handled by arm 1, not the orphan arm — the gate must
--    suppress the orphan arm yet the group must still move.  Also a real delete.
DROP TABLE IF EXISTS ntg2_f, ntg2_d CASCADE;
CREATE TABLE ntg2_d(id int primary key, label text);
CREATE TABLE ntg2_f(id int primary key, did int, amt numeric);
INSERT INTO ntg2_d VALUES (1,'L1'),(2,'L2');
INSERT INTO ntg2_f VALUES (1,1,10),(2,1,20),(3,2,30),(4,NULL,40),(5,7,50);
CREATE MATERIALIZED VIEW ntg2_i WITH (incremental_refresh=true) AS
  SELECT d.label gk, sum(f.amt) s, count(*) c
  FROM ntg2_f f LEFT JOIN ntg2_d d ON f.did=d.id GROUP BY d.label;
CREATE MATERIALIZED VIEW ntg2_o AS
  SELECT d.label gk, sum(f.amt) s, count(*) c
  FROM ntg2_f f LEFT JOIN ntg2_d d ON f.did=d.id GROUP BY d.label;
UPDATE ntg2_d SET label='L1new' WHERE id=1;     -- group-key rename: arm 1 moves group, gate suppresses orphan arm
DELETE FROM ntg2_d WHERE id=2;                   -- real orphan
INSERT INTO ntg2_d VALUES (7,'L7');              -- de-orphans ntg2_f 5 (did=7 was dangling)
REFRESH MATERIALIZED VIEW ntg2_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,s,c FROM ntg2_i EXCEPT SELECT gk,s,c FROM ntg2_o) UNION ALL
    (SELECT gk,s,c FROM ntg2_o EXCEPT SELECT gk,s,c FROM ntg2_i)) z;
  IF d=0 THEN RAISE NOTICE 'direct-dim key rename (arm 1) + orphan/de-orphan == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'NULL-transition gate direct dim: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ntg2_f, ntg2_d CASCADE;

-- 3. Fallback: an OR-connected join qual makes the delta table's join columns
--    non-cleanly enumerable, so the gate declines and the orphan arm stays
--    unconditional.  A rename here still runs the (unrestricted) NULL arm, but
--    correctness is unchanged — must equal REFRESH.
DROP TABLE IF EXISTS ntg3_f, ntg3_d CASCADE;
CREATE TABLE ntg3_d(id int primary key, alt int, code text);
CREATE TABLE ntg3_f(id int primary key, pid int, amt numeric);
INSERT INTO ntg3_d VALUES (1,NULL,'X'),(2,5,'Y');
INSERT INTO ntg3_f VALUES (1,1,10),(2,5,20),(3,8,5);
CREATE MATERIALIZED VIEW ntg3_i WITH (incremental_refresh=true) AS
  SELECT d.code gk, sum(f.amt) s FROM ntg3_f f LEFT JOIN ntg3_d d ON (f.pid=d.id OR f.pid=d.alt) GROUP BY d.code;
CREATE MATERIALIZED VIEW ntg3_o AS
  SELECT d.code gk, sum(f.amt) s FROM ntg3_f f LEFT JOIN ntg3_d d ON (f.pid=d.id OR f.pid=d.alt) GROUP BY d.code;
UPDATE ntg3_d SET code='Xr' WHERE id=1;          -- rename under an OR join: gate declines, still correct
DELETE FROM ntg3_d WHERE id=2;                   -- orphans f(2) (reached only via alt)
REFRESH MATERIALIZED VIEW ntg3_o;
DO $$DECLARE d int; BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,s FROM ntg3_i EXCEPT SELECT gk,s FROM ntg3_o) UNION ALL
    (SELECT gk,s FROM ntg3_o EXCEPT SELECT gk,s FROM ntg3_i)) z;
  IF d=0 THEN RAISE NOTICE 'OR-qual join (gate declines, arm unconditional) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'NULL-transition gate OR fallback: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ntg3_f, ntg3_d CASCADE;

\echo ''
\echo '=== NULL-transition gate test complete ==='
