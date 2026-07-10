-- DBblue IVM — M5 anchor-restriction correctness (perf optimization, matview_incr.c
-- incr_plan_anchor_restrict / incr_build_dep_path / incr_find_equality_hop).
--
-- A delta on a deep/dimension table used to make arm 1 (and the orphan arm,
-- which shares its query) scan the WHOLE fact side through the LEFT chain,
-- because the deparse-based delta swap only replaces the delta table's RTE —
-- it doesn't touch the join tree's shape, so the FROM-clause anchor (the fact
-- table) stays unrestricted regardless of how small the swapped ENR is
-- (measured 300-570ms on 500k facts, vs ~12ms for a fact-table insert).
--
-- Fix: walk the join-ancestry chain from the delta table up to the anchor
-- (reusing the same qual-walk incr_table_at_or_below already does), and when
-- every hop is a clean "=" equality, inject "anchor.col IN (SELECT ak FROM
-- _aff_anchor_)" — a nested chain built from the delta ENR up through each
-- live intermediate table — into the SAME deparsed query arm 1 and the orphan
-- arm both read, restricting the fact scan to only genuinely-connected rows.
-- The restriction is always a SUPERSET filter (an ambiguous/OR/non-equality
-- hop, or a partial composite-key match, only widens candidate rows — never
-- excludes a legitimately affected one, since the real join conditions are
-- still re-applied in full downstream) and falls back to the original,
-- unrestricted, always-correct scan whenever a clean path can't be built.
--
-- Every case is checked == a full REFRESH, including deltas that create real
-- orphan/de-orphan transitions and ones that don't (plain renames).
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: M5 anchor-restriction (deep-dimension delta) ==='
\echo ''

-- 1. Multi-hop chain (fact -> prod -> tmpl): the exact shape that motivated
--    this optimization.  A rename must NOT spuriously appear as an orphan
--    transition; a real delete/insert on the deep table must.
DROP TABLE IF EXISTS ar_f, ar_p, ar_t CASCADE;
CREATE TABLE ar_t(id int primary key, code text);
CREATE TABLE ar_p(id int primary key, tmpl int);
CREATE TABLE ar_f(id int primary key, pid int, amt numeric);
INSERT INTO ar_t VALUES (1,'A'),(2,'B'),(3,'C');
INSERT INTO ar_p VALUES (10,1),(11,2),(12,NULL);
INSERT INTO ar_f VALUES (1,10,5),(2,10,7),(3,11,3),(4,12,9),(5,NULL,1);
CREATE MATERIALIZED VIEW ar1_i WITH (incremental_refresh=true) AS
  SELECT t.code gk, sum(f.amt) s, count(*) c
  FROM ar_f f LEFT JOIN ar_p p ON f.pid=p.id LEFT JOIN ar_t t ON p.tmpl=t.id
  GROUP BY t.code;
CREATE MATERIALIZED VIEW ar1_o AS
  SELECT t.code gk, sum(f.amt) s, count(*) c
  FROM ar_f f LEFT JOIN ar_p p ON f.pid=p.id LEFT JOIN ar_t t ON p.tmpl=t.id
  GROUP BY t.code;
UPDATE ar_t SET code='A2' WHERE id=1;        -- rename: NOT an orphan transition
DELETE FROM ar_t WHERE id=2;                 -- real delete: ar_p(11) orphans
UPDATE ar_p SET tmpl=3 WHERE id=12;           -- de-orphans ar_f(4) into group C
INSERT INTO ar_t VALUES (4,'D');
INSERT INTO ar_p VALUES (13,4);
INSERT INTO ar_f VALUES (6,13,2);
REFRESH MATERIALIZED VIEW ar1_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,s,c FROM ar1_i EXCEPT SELECT gk,s,c FROM ar1_o) UNION ALL
    (SELECT gk,s,c FROM ar1_o EXCEPT SELECT gk,s,c FROM ar1_i)) z;
  IF d=0 THEN RAISE NOTICE 'multi-hop deep-dimension delta (anchor-restricted) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'anchor-restriction 2-hop: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ar_f, ar_p, ar_t CASCADE;

-- 2. Independent optional branches (multi-hop product chain + a DIRECT
--    optional currency), matching the RW-02 topology — the two per-delta-table
--    anchor restrictions must not interfere with each other.
DROP TABLE IF EXISTS ar_f2, ar_p2, ar_t2, ar_c2 CASCADE;
CREATE TABLE ar_t2(id int primary key, code text);
CREATE TABLE ar_p2(id int primary key, tmpl int);
CREATE TABLE ar_c2(id int primary key, symbol text);
CREATE TABLE ar_f2(id int primary key, pid int, cid int, amt numeric);
INSERT INTO ar_t2 VALUES (1,'A'),(2,'B');
INSERT INTO ar_p2 VALUES (10,1),(11,2);
INSERT INTO ar_c2 VALUES (100,'USD'),(101,'EUR');
INSERT INTO ar_f2 VALUES (1,10,100,5),(2,11,101,7),(3,NULL,NULL,3);
CREATE MATERIALIZED VIEW ar2_i WITH (incremental_refresh=true) AS
  SELECT t.code tc, c.symbol cs, sum(f.amt) s, count(*) cnt
  FROM ar_f2 f LEFT JOIN ar_p2 p ON f.pid=p.id LEFT JOIN ar_t2 t ON p.tmpl=t.id
              LEFT JOIN ar_c2 c ON f.cid=c.id
  GROUP BY t.code, c.symbol;
CREATE MATERIALIZED VIEW ar2_o AS
  SELECT t.code tc, c.symbol cs, sum(f.amt) s, count(*) cnt
  FROM ar_f2 f LEFT JOIN ar_p2 p ON f.pid=p.id LEFT JOIN ar_t2 t ON p.tmpl=t.id
              LEFT JOIN ar_c2 c ON f.cid=c.id
  GROUP BY t.code, c.symbol;
DELETE FROM ar_t2 WHERE id=1;         -- multi-hop orphan (via prod)
DELETE FROM ar_c2 WHERE id=101;       -- direct orphan (currency)
INSERT INTO ar_c2 VALUES (102,'GBP');
UPDATE ar_f2 SET cid=102 WHERE id=1;
REFRESH MATERIALIZED VIEW ar2_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT tc,cs,s,cnt FROM ar2_i EXCEPT SELECT tc,cs,s,cnt FROM ar2_o) UNION ALL
    (SELECT tc,cs,s,cnt FROM ar2_o EXCEPT SELECT tc,cs,s,cnt FROM ar2_i)) z;
  IF d=0 THEN RAISE NOTICE 'independent optional branches (anchor-restricted) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'anchor-restriction independent branches: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ar_f2, ar_p2, ar_t2, ar_c2 CASCADE;

-- 3. Fallback correctness: shapes the anchor-restriction must NOT (unsafely)
--    apply to — an OR-connected join qual and a mixed equality/inequality
--    qual.  Must still be maintained correctly (via the always-correct
--    unrestricted path, or a safe superset restriction on just the "="
--    conjunct); never wrong, never an internal error.
DROP TABLE IF EXISTS ar_f3, ar_d3 CASCADE;
CREATE TABLE ar_d3(id int primary key, alt_id int, code text);
CREATE TABLE ar_f3(id int primary key, pid int, amt numeric);
INSERT INTO ar_d3 VALUES (1,NULL,'X'),(2,5,'Y');
INSERT INTO ar_f3 VALUES (1,1,10),(2,5,20);   -- f(2) reaches d(2) via alt_id
CREATE MATERIALIZED VIEW ar3_i WITH (incremental_refresh=true) AS
  SELECT d.code gk, sum(f.amt) s
  FROM ar_f3 f LEFT JOIN ar_d3 d ON (f.pid=d.id OR f.pid=d.alt_id)
  GROUP BY d.code;
CREATE MATERIALIZED VIEW ar3_o AS
  SELECT d.code gk, sum(f.amt) s
  FROM ar_f3 f LEFT JOIN ar_d3 d ON (f.pid=d.id OR f.pid=d.alt_id)
  GROUP BY d.code;
DELETE FROM ar_d3 WHERE id=2;    -- orphans f(2), reached only via the OR's alt_id side
INSERT INTO ar_d3 VALUES (3,1,'Z');
REFRESH MATERIALIZED VIEW ar3_o;
DO $$
DECLARE d int;
BEGIN
  SELECT count(*) INTO d FROM (
    (SELECT gk,s FROM ar3_i EXCEPT SELECT gk,s FROM ar3_o) UNION ALL
    (SELECT gk,s FROM ar3_o EXCEPT SELECT gk,s FROM ar3_i)) z;
  IF d=0 THEN RAISE NOTICE 'OR-qual join (anchor-restriction correctly declines) == REFRESH: PASS';
  ELSE RAISE EXCEPTION 'OR-qual fallback: FAIL (% rows differ)', d; END IF;
END$$;
DROP TABLE ar_f3, ar_d3 CASCADE;

\echo ''
\echo '=== M5 anchor-restriction test complete ==='
