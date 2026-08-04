--
-- DBblue COUNT cache and OFFSET-flip
--
-- Every check here asserts on the *answer*, never on log output or timing, so
-- the expected file stays deterministic.  The cache is only ever correct if a
-- cached count equals what a fresh scan would return, so comparing answers is
-- also exactly the property that matters.
--

SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

CREATE TABLE dbb_cc (id int PRIMARY KEY, grp int, payload text);
INSERT INTO dbb_cc SELECT g, g % 10, 'row' || g FROM generate_series(1, 100) g;

-- Warm the cache, then confirm each kind of local write is reflected.
SELECT count(*) FROM dbb_cc WHERE id > 0;

INSERT INTO dbb_cc SELECT g, g % 10, 'row' || g FROM generate_series(101, 120) g;
SELECT count(*) AS after_insert FROM dbb_cc WHERE id > 0;

DELETE FROM dbb_cc WHERE id > 110;
SELECT count(*) AS after_delete FROM dbb_cc WHERE id > 0;

-- An UPDATE that moves rows across the predicate boundary must be reflected.
SELECT count(*) FROM dbb_cc WHERE grp = 3;
UPDATE dbb_cc SET grp = 3 WHERE grp = 4;
SELECT count(*) AS after_update FROM dbb_cc WHERE grp = 3;

--
-- A count captured by a transaction that then rolls back must not be cached:
-- it counted rows that no longer exist.
--
BEGIN;
INSERT INTO dbb_cc SELECT g, 0, 'x' FROM generate_series(500, 560) g;
SELECT count(*) AS inside_txn FROM dbb_cc WHERE id > 0;
ROLLBACK;
SELECT count(*) AS after_rollback FROM dbb_cc WHERE id > 0;

-- Same, via subtransaction rollback.
BEGIN;
SAVEPOINT sp;
INSERT INTO dbb_cc SELECT g, 0, 'x' FROM generate_series(600, 660) g;
SELECT count(*) AS inside_subxact FROM dbb_cc WHERE id > 0;
ROLLBACK TO SAVEPOINT sp;
COMMIT;
SELECT count(*) AS after_rollback_to_savepoint FROM dbb_cc WHERE id > 0;

-- Same, via a PL/pgSQL EXCEPTION block (an implicit subtransaction).
DO $$
BEGIN
    INSERT INTO dbb_cc SELECT g, 0, 'x' FROM generate_series(700, 760) g;
    PERFORM count(*) FROM dbb_cc WHERE id > 0;
    RAISE EXCEPTION 'rollback me';
EXCEPTION WHEN OTHERS THEN
    NULL;
END $$;
SELECT count(*) AS after_plpgsql_exception FROM dbb_cc WHERE id > 0;

--
-- TRUNCATE changes the count drastically and must invalidate.
--
CREATE TABLE dbb_cc_tr (id int);
INSERT INTO dbb_cc_tr SELECT generate_series(1, 50);
SELECT count(*) FROM dbb_cc_tr WHERE id > 0;
TRUNCATE dbb_cc_tr;
SELECT count(*) AS after_truncate FROM dbb_cc_tr WHERE id > 0;

--
-- A STABLE function in the predicate must not be cached: the fingerprint
-- hashes the expression, so one key would serve every role.
--
CREATE ROLE regress_dbb_alice;
CREATE ROLE regress_dbb_bob;
CREATE TABLE dbb_cc_own (id int, owner name);
INSERT INTO dbb_cc_own VALUES
    (1, 'regress_dbb_alice'), (2, 'regress_dbb_alice'), (3, 'regress_dbb_alice'),
    (4, 'regress_dbb_bob'), (5, 'regress_dbb_bob');
GRANT SELECT ON dbb_cc_own TO regress_dbb_alice, regress_dbb_bob;

SET ROLE regress_dbb_alice;
SELECT count(*) AS alice_sees FROM dbb_cc_own WHERE owner = current_user;
SET ROLE regress_dbb_bob;
SELECT count(*) AS bob_sees FROM dbb_cc_own WHERE owner = current_user;
RESET ROLE;

--
-- Parameterised predicates are rejected, so distinct parameter values must not
-- collide on one cache entry.
--
PREPARE dbb_p(int) AS SELECT count(*) FROM dbb_cc WHERE grp = $1;
EXECUTE dbb_p(3);
EXECUTE dbb_p(5);
EXECUTE dbb_p(3);
DEALLOCATE dbb_p;

--
-- OFFSET-flip: the rewritten plan must return exactly the rows the plain plan
-- would.  Compared as a set difference in both directions, so a shifted window
-- shows up as non-zero.
--
CREATE TABLE dbb_pg (id int PRIMARY KEY, payload text);
INSERT INTO dbb_pg SELECT g, 'r' || g FROM generate_series(1, 200) g;
ANALYZE dbb_pg;

-- warm N for the predicate the paginated query uses
SELECT count(*) FROM dbb_pg WHERE id > 0;

-- deep page, well past the midpoint so the flip is eligible
SELECT id FROM dbb_pg WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 180;

-- last partial page: fewer rows remain than LIMIT
SELECT id FROM dbb_pg WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 197;

-- descending order, and a multi-key sort
SELECT id FROM dbb_pg WHERE id > 0 ORDER BY id DESC LIMIT 5 OFFSET 180;
SELECT id FROM dbb_pg WHERE id > 0 ORDER BY payload, id LIMIT 3 OFFSET 190;

-- differential check against the same query with the feature off
CREATE TEMP TABLE flip_on AS
    SELECT id FROM dbb_pg WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 180;
SET dbblue_offset_flip = off;
SET dbblue_count_cache = off;
CREATE TEMP TABLE flip_off AS
    SELECT id FROM dbb_pg WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 180;
SELECT (SELECT count(*) FROM (SELECT id FROM flip_on EXCEPT SELECT id FROM flip_off) a)
     + (SELECT count(*) FROM (SELECT id FROM flip_off EXCEPT SELECT id FROM flip_on) b)
    AS rows_differing;
SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

--
-- Shapes that must be rejected, and must still answer correctly.
--
-- no WHERE clause at all
SELECT count(*) AS unfiltered FROM dbb_pg;
-- volatile function
SELECT count(*) AS volatile_qual FROM dbb_pg WHERE id > 0 AND random() <= 1.0;
-- subquery in the predicate
SELECT count(*) AS sublink FROM dbb_pg WHERE id IN (SELECT id FROM dbb_cc);
-- join
SELECT count(*) AS joined FROM dbb_pg p JOIN dbb_cc c ON c.id = p.id;
-- a system catalog
SELECT count(*) > 0 AS catalog_ok FROM pg_class WHERE relkind = 'r';

--
-- Partitioned tables are excluded; writes bump the leaf's stamp, not the
-- parent's, so the parent must never serve a cached count.
--
CREATE TABLE dbb_part (id int, val int) PARTITION BY RANGE (id);
CREATE TABLE dbb_part_1 PARTITION OF dbb_part FOR VALUES FROM (1) TO (100);
CREATE TABLE dbb_part_2 PARTITION OF dbb_part FOR VALUES FROM (100) TO (200);
INSERT INTO dbb_part SELECT g, g FROM generate_series(1, 150) g;
SELECT count(*) FROM dbb_part WHERE id > 0;
INSERT INTO dbb_part_1 SELECT g, g FROM generate_series(50, 59) g;
SELECT count(*) AS after_leaf_insert FROM dbb_part WHERE id > 0;

--
-- The GUCs must be independently controllable, and the flip must be inert
-- without the cache to supply N.
--
SET dbblue_count_cache = off;
SET dbblue_offset_flip = on;
SELECT id FROM dbb_pg WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 180;

RESET dbblue_count_cache;
RESET dbblue_offset_flip;

DROP TABLE dbb_pg, dbb_cc, dbb_cc_tr, dbb_cc_own, dbb_part;
DROP ROLE regress_dbb_alice, regress_dbb_bob;
