--
-- DBblue: REFRESH MATERIALIZED VIEW skip-if-unchanged (auto_skip_unchanged)
--
-- Every case asserts two things: that the contents match ground truth, and --
-- via the DEBUG1 decision line -- which path the refresh actually took.  The
-- second assertion is what stops these from being vacuous: a test that only
-- checks the answer still passes when the optimization never engages, and so
-- would not notice the feature silently doing nothing.
--
-- All objects are prefixed mvs_ so this file is self-contained.

SET client_min_messages = debug1;

-- ---------------------------------------------------------------- basics
CREATE TABLE mvs_t(id int, v int);
INSERT INTO mvs_t SELECT g, g FROM generate_series(1, 20) g;

-- CREATE ... WITH DATA records a watermark, so the first REFRESH can skip.
CREATE MATERIALIZED VIEW mvs_mv WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c, sum(v) AS s FROM mvs_t;

REFRESH MATERIALIZED VIEW mvs_mv;                 -- skipped
SELECT c, s FROM mvs_mv;

INSERT INTO mvs_t VALUES (21, 21);
REFRESH MATERIALIZED VIEW mvs_mv;                 -- a source was written
SELECT c = (SELECT count(*) FROM mvs_t) AS matches FROM mvs_mv;

REFRESH MATERIALIZED VIEW mvs_mv;                 -- skipped again
UPDATE mvs_t SET v = v * 2 WHERE id <= 3;
REFRESH MATERIALIZED VIEW mvs_mv;                 -- a source was written
DELETE FROM mvs_t WHERE id = 21;
REFRESH MATERIALIZED VIEW mvs_mv;                 -- a source was written
SELECT s = (SELECT sum(v) FROM mvs_t) AS matches FROM mvs_mv;

-- The GUC disables it outright.
SET dbblue_matview_skip_unchanged = off;
REFRESH MATERIALIZED VIEW mvs_mv;                 -- GUC off
SET dbblue_matview_skip_unchanged = on;

-- Without the reloption nothing is logged at all.
CREATE MATERIALIZED VIEW mvs_plain AS SELECT count(*) AS c FROM mvs_t;
REFRESH MATERIALIZED VIEW mvs_plain;
DROP MATERIALIZED VIEW mvs_plain;

-- --------------------------------------------- source set enumeration
-- A source reached only through a subquery, a CTE or a SubLink must still be
-- tracked; enumerating only the top-level range table missed all three, and an
-- empty source set used to read as "clean" and never refresh again.
CREATE TABLE mvs_sub(id int);
INSERT INTO mvs_sub SELECT g FROM generate_series(1, 10) g;

CREATE MATERIALIZED VIEW mvs_from WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c FROM (SELECT * FROM mvs_sub WHERE id > 0) x;
CREATE MATERIALIZED VIEW mvs_cte WITH (auto_skip_unchanged=true) AS
  WITH cte AS (SELECT * FROM mvs_sub) SELECT count(*) AS c FROM cte;
CREATE MATERIALIZED VIEW mvs_sublink WITH (auto_skip_unchanged=true) AS
  SELECT (SELECT count(*) FROM mvs_sub) AS c;

REFRESH MATERIALIZED VIEW mvs_from;               -- skipped
REFRESH MATERIALIZED VIEW mvs_cte;                -- skipped
REFRESH MATERIALIZED VIEW mvs_sublink;            -- skipped

INSERT INTO mvs_sub SELECT g FROM generate_series(11, 25) g;
REFRESH MATERIALIZED VIEW mvs_from;               -- a source was written
REFRESH MATERIALIZED VIEW mvs_cte;                -- a source was written
REFRESH MATERIALIZED VIEW mvs_sublink;            -- a source was written
SELECT (SELECT c FROM mvs_from) AS f, (SELECT c FROM mvs_cte) AS c,
       (SELECT c FROM mvs_sublink) AS s, count(*) AS truth FROM mvs_sub;

-- ------------------------------------------------------- shared source
-- Cleanliness is per (matview, source).  With a per-source flag the first
-- matview to refresh consumed it and the rest were starved, so a plain
-- "refresh everything" loop silently froze all but one.
CREATE TABLE mvs_shared(id int);
INSERT INTO mvs_shared SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_r1 WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_shared;
CREATE MATERIALIZED VIEW mvs_r2 WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_shared;
CREATE MATERIALIZED VIEW mvs_r3 WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_shared;

INSERT INTO mvs_shared SELECT g FROM generate_series(11, 30) g;
REFRESH MATERIALIZED VIEW mvs_r1;                 -- a source was written
REFRESH MATERIALIZED VIEW mvs_r2;                 -- a source was written, not "clean"
REFRESH MATERIALIZED VIEW mvs_r3;                 -- a source was written, not "clean"
SELECT (SELECT c FROM mvs_r1) AS r1, (SELECT c FROM mvs_r2) AS r2,
       (SELECT c FROM mvs_r3) AS r3, count(*) AS truth FROM mvs_shared;

-- ------------------------------------------------------------ TRUNCATE
-- TRUNCATE never reaches the heapam write hooks; it is caught because it
-- changes the source's relfilenumber.
CREATE TABLE mvs_trunc(id int);
INSERT INTO mvs_trunc SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_mvtrunc WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_trunc;
REFRESH MATERIALIZED VIEW mvs_mvtrunc;            -- skipped
TRUNCATE mvs_trunc;
REFRESH MATERIALIZED VIEW mvs_mvtrunc;            -- a source's storage changed
SELECT c FROM mvs_mvtrunc;

-- ---------------------------------------------------------- partitions
-- Writes land on leaves, not the parent, so leaves are what gets tracked.
-- Membership changes are caught by comparing the recorded source set.
CREATE TABLE mvs_p(id int) PARTITION BY RANGE (id);
CREATE TABLE mvs_p_a PARTITION OF mvs_p FOR VALUES FROM (0) TO (100);
CREATE TABLE mvs_p_b PARTITION OF mvs_p FOR VALUES FROM (100) TO (200);
INSERT INTO mvs_p SELECT g FROM generate_series(1, 50) g;
CREATE MATERIALIZED VIEW mvs_mvp WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_p;

REFRESH MATERIALIZED VIEW mvs_mvp;                -- skipped
INSERT INTO mvs_p VALUES (150);                   -- routed to a leaf
REFRESH MATERIALIZED VIEW mvs_mvp;                -- a source was written
INSERT INTO mvs_p_a VALUES (60);                  -- straight into the leaf
REFRESH MATERIALIZED VIEW mvs_mvp;                -- a source was written
SELECT c = (SELECT count(*) FROM mvs_p) AS matches FROM mvs_mvp;

ALTER TABLE mvs_p DETACH PARTITION mvs_p_b;
REFRESH MATERIALIZED VIEW mvs_mvp;                -- the source set changed
SELECT c = (SELECT count(*) FROM mvs_p) AS matches FROM mvs_mvp;

ALTER TABLE mvs_p ATTACH PARTITION mvs_p_b FOR VALUES FROM (100) TO (200);
REFRESH MATERIALIZED VIEW mvs_mvp;                -- the source set changed
SELECT c = (SELECT count(*) FROM mvs_p) AS matches FROM mvs_mvp;

DROP TABLE mvs_p_b;
REFRESH MATERIALIZED VIEW mvs_mvp;                -- the source set changed
SELECT c = (SELECT count(*) FROM mvs_p) AS matches FROM mvs_mvp;

-- --------------------------------------------------------------- views
-- The stored rule query is NOT rewritten, so a source view appears in it as a
-- storage-less RTE_RELATION with its base tables absent.  Enumeration rewrites
-- first, which is also what makes redefinition detectable.
CREATE TABLE mvs_vbase(id int, v int);
INSERT INTO mvs_vbase SELECT g, g FROM generate_series(1, 20) g;
CREATE VIEW mvs_v AS SELECT * FROM mvs_vbase WHERE v > 0;
CREATE MATERIALIZED VIEW mvs_mvv WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_v;

REFRESH MATERIALIZED VIEW mvs_mvv;                -- skipped
INSERT INTO mvs_vbase VALUES (21, 21);
REFRESH MATERIALIZED VIEW mvs_mvv;                -- a source was written
SELECT c = (SELECT count(*) FROM mvs_v) AS matches FROM mvs_mvv;

CREATE OR REPLACE VIEW mvs_v AS SELECT * FROM mvs_vbase WHERE v > 10;
REFRESH MATERIALIZED VIEW mvs_mvv;                -- the definition changed
SELECT c = (SELECT count(*) FROM mvs_v) AS matches FROM mvs_mvv;

-- ------------------------------------------------- matview over matview
-- A non-concurrent refresh writes to a transient heap, so the inner matview's
-- own relid is never noted; its relfilenumber changing is what catches it.
CREATE TABLE mvs_inner_src(id int);
INSERT INTO mvs_inner_src SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_inner AS SELECT id FROM mvs_inner_src;
CREATE MATERIALIZED VIEW mvs_outer WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_inner;

REFRESH MATERIALIZED VIEW mvs_outer;              -- skipped
INSERT INTO mvs_inner_src VALUES (11);
REFRESH MATERIALIZED VIEW mvs_inner;
REFRESH MATERIALIZED VIEW mvs_outer;              -- a source's storage changed
SELECT c = (SELECT count(*) FROM mvs_inner) AS matches FROM mvs_outer;

-- ---------------------------------------------------- transaction rules
-- A rolled-back refresh must not leave the matview looking up to date, and a
-- refresh in the same transaction as a write to its own source must not skip.
CREATE TABLE mvs_tx(id int);
INSERT INTO mvs_tx SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_mvtx WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_tx;
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- skipped

INSERT INTO mvs_tx SELECT g FROM generate_series(11, 30) g;
BEGIN;
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- a source was written
ROLLBACK;
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- must NOT be skipped
SELECT c = (SELECT count(*) FROM mvs_tx) AS matches FROM mvs_mvtx;

BEGIN;
SAVEPOINT sp;
REFRESH MATERIALIZED VIEW mvs_mvtx;
ROLLBACK TO sp;
COMMIT;
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- must NOT be skipped
SELECT c = (SELECT count(*) FROM mvs_tx) AS matches FROM mvs_mvtx;

BEGIN;
INSERT INTO mvs_tx VALUES (99);
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- this transaction wrote it
COMMIT;
SELECT c = (SELECT count(*) FROM mvs_tx) AS matches FROM mvs_mvtx;

-- WITH NO DATA leaves the matview unpopulated; the next plain REFRESH must
-- repopulate it rather than skip and strand it unreadable forever.
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- skipped
REFRESH MATERIALIZED VIEW mvs_mvtx WITH NO DATA;
SELECT relispopulated FROM pg_class WHERE relname = 'mvs_mvtx';
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- must rebuild
SELECT relispopulated FROM pg_class WHERE relname = 'mvs_mvtx';
SELECT c = (SELECT count(*) FROM mvs_tx) AS matches FROM mvs_mvtx;

-- Isolation levels that pin a transaction snapshot decline both branches.
BEGIN ISOLATION LEVEL REPEATABLE READ;
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- fixed transaction snapshot
COMMIT;
BEGIN ISOLATION LEVEL SERIALIZABLE;
REFRESH MATERIALIZED VIEW mvs_mvtx;               -- fixed transaction snapshot
COMMIT;
SELECT c = (SELECT count(*) FROM mvs_tx) AS matches FROM mvs_mvtx;

-- ------------------------------------------------- storage-only rewrites
-- VACUUM FULL does not change the contents but does change the relfilenumber,
-- so it costs one extra rebuild.  What matters is that tracking still works
-- afterwards rather than the matview becoming permanently stuck.
CREATE TABLE mvs_vac(id int);
INSERT INTO mvs_vac SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_mvvac WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_vac;
REFRESH MATERIALIZED VIEW mvs_mvvac;              -- skipped
VACUUM FULL mvs_vac;
REFRESH MATERIALIZED VIEW mvs_mvvac;              -- a source's storage changed
REFRESH MATERIALIZED VIEW mvs_mvvac;              -- skipped again
INSERT INTO mvs_vac VALUES (11);
REFRESH MATERIALIZED VIEW mvs_mvvac;              -- a source was written
SELECT c = (SELECT count(*) FROM mvs_vac) AS matches FROM mvs_mvvac;

-- An UNLOGGED source is tracked the same way as a logged one.
CREATE UNLOGGED TABLE mvs_unlogged(id int);
INSERT INTO mvs_unlogged SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_mvunlogged WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c FROM mvs_unlogged;
REFRESH MATERIALIZED VIEW mvs_mvunlogged;         -- skipped
INSERT INTO mvs_unlogged SELECT g FROM generate_series(11, 20) g;
REFRESH MATERIALIZED VIEW mvs_mvunlogged;         -- a source was written
SELECT c = (SELECT count(*) FROM mvs_unlogged) AS matches FROM mvs_mvunlogged;

-- ---------------------------------------------------- the reject list
-- Each of these changes what the matview should contain with no tracked write,
-- so each must refuse to skip.  The DEBUG line names which rule fired.
CREATE SEQUENCE mvs_seq;
CREATE MATERIALIZED VIEW mvs_mvseq WITH (auto_skip_unchanged=true) AS SELECT last_value AS c FROM mvs_seq;
REFRESH MATERIALIZED VIEW mvs_mvseq;              -- not an ordinary table
SELECT nextval('mvs_seq');
REFRESH MATERIALIZED VIEW mvs_mvseq;              -- not an ordinary table
SELECT c = (SELECT last_value FROM mvs_seq) AS matches FROM mvs_mvseq;

CREATE MATERIALIZED VIEW mvs_mvcat WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c FROM pg_class WHERE relname LIKE 'mvs\_bump%';
REFRESH MATERIALIZED VIEW mvs_mvcat;              -- system catalog
CREATE TABLE mvs_bump_a(id int);
REFRESH MATERIALIZED VIEW mvs_mvcat;              -- system catalog
SELECT c FROM mvs_mvcat;

CREATE MATERIALIZED VIEW mvs_mvnow WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c FROM mvs_t WHERE now() IS NOT NULL;
REFRESH MATERIALIZED VIEW mvs_mvnow;              -- not IMMUTABLE
REFRESH MATERIALIZED VIEW mvs_mvnow;              -- not IMMUTABLE

CREATE MATERIALIZED VIEW mvs_mvsample WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c FROM mvs_t TABLESAMPLE BERNOULLI (50);
REFRESH MATERIALIZED VIEW mvs_mvsample;           -- TABLESAMPLE
REFRESH MATERIALIZED VIEW mvs_mvsample;           -- TABLESAMPLE

CREATE TABLE mvs_vgen(id int, base int, vg int GENERATED ALWAYS AS (base * 2) VIRTUAL);
INSERT INTO mvs_vgen(id, base) SELECT g, g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_mvvgen WITH (auto_skip_unchanged=true) AS SELECT sum(vg) AS c FROM mvs_vgen;
REFRESH MATERIALIZED VIEW mvs_mvvgen;             -- virtual generated column
ALTER TABLE mvs_vgen ALTER COLUMN vg SET EXPRESSION AS (base * 100);
REFRESH MATERIALIZED VIEW mvs_mvvgen;             -- virtual generated column
SELECT c = (SELECT sum(vg) FROM mvs_vgen) AS matches FROM mvs_mvvgen;

CREATE TABLE mvs_rls(id int, tenant int);
INSERT INTO mvs_rls SELECT g, g % 2 FROM generate_series(1, 20) g;
ALTER TABLE mvs_rls ENABLE ROW LEVEL SECURITY;
CREATE MATERIALIZED VIEW mvs_mvrls WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_rls;
REFRESH MATERIALIZED VIEW mvs_mvrls;              -- row-level security
REFRESH MATERIALIZED VIEW mvs_mvrls;              -- row-level security

CREATE TABLE mvs_srf_src(id int);
INSERT INTO mvs_srf_src SELECT g FROM generate_series(1, 5) g;
CREATE MATERIALIZED VIEW mvs_mvsrf WITH (auto_skip_unchanged=true) AS
  SELECT count(*) AS c FROM generate_series(1, 5) g;
REFRESH MATERIALIZED VIEW mvs_mvsrf;              -- reads something else
REFRESH MATERIALIZED VIEW mvs_mvsrf;              -- reads something else

-- A matview populated while the feature was switched off has no watermark, so
-- the first refresh after switching it back on has nothing to compare against.
SET dbblue_matview_skip_unchanged = off;
CREATE MATERIALIZED VIEW mvs_nowm WITH (auto_skip_unchanged=true) AS SELECT count(*) AS c FROM mvs_t;
SET dbblue_matview_skip_unchanged = on;
REFRESH MATERIALIZED VIEW mvs_nowm;               -- no watermark yet
REFRESH MATERIALIZED VIEW mvs_nowm;               -- skipped
SELECT c = (SELECT count(*) FROM mvs_t) AS matches FROM mvs_nowm;

-- More sources than can be tracked per matview must refuse rather than
-- truncate the set and compare a prefix.
SELECT format('CREATE TABLE mvs_many_%s(id int);', g) FROM generate_series(1, 40) g \gexec
SELECT 'CREATE MATERIALIZED VIEW mvs_mvmany WITH (auto_skip_unchanged=true) AS SELECT '
       || string_agg(format('(SELECT count(*) FROM mvs_many_%s) AS c%s', g, g), ', ')
  FROM generate_series(1, 40) g \gexec
REFRESH MATERIALIZED VIEW mvs_mvmany;             -- too many sources
REFRESH MATERIALIZED VIEW mvs_mvmany;             -- too many sources
DROP MATERIALIZED VIEW mvs_mvmany;
SELECT format('DROP TABLE mvs_many_%s;', g) FROM generate_series(1, 40) g \gexec

RESET client_min_messages;

-- ----------------------------------------------------------- CONCURRENTLY
-- Excluded from both branches: refresh_by_match_merge updates in place, so the
-- matview's relfilenumber does not change and there is no commit witness.
--
-- Deliberately NOT run at debug1.  The concurrent path builds a transient table
-- whose toast index is named after its OID, and PostgreSQL logs that name at
-- DEBUG1 -- which differs on every run.  There is no decision line to assert
-- here anyway, because the gate is not consulted at all for CONCURRENTLY.
CREATE TABLE mvs_conc(id int);
INSERT INTO mvs_conc SELECT g FROM generate_series(1, 10) g;
CREATE MATERIALIZED VIEW mvs_mvconc WITH (auto_skip_unchanged=true) AS SELECT id FROM mvs_conc;
CREATE UNIQUE INDEX mvs_mvconc_idx ON mvs_mvconc(id);
INSERT INTO mvs_conc VALUES (11);
REFRESH MATERIALIZED VIEW CONCURRENTLY mvs_mvconc;
INSERT INTO mvs_conc VALUES (12);
REFRESH MATERIALIZED VIEW CONCURRENTLY mvs_mvconc;
SELECT count(*) = (SELECT count(*) FROM mvs_conc) AS matches FROM mvs_mvconc;

-- A plain REFRESH afterwards must still be correct: the concurrent refreshes
-- wrote no watermark, so this one has to rebuild.
SET client_min_messages = debug1;
INSERT INTO mvs_conc VALUES (13);
REFRESH MATERIALIZED VIEW mvs_mvconc;
SELECT count(*) = (SELECT count(*) FROM mvs_conc) AS matches FROM mvs_mvconc;
RESET client_min_messages;

-- ------------------------------------------------------------- cleanup
DROP MATERIALIZED VIEW mvs_mvunlogged, mvs_mvvac, mvs_mvconc, mvs_mvsrf, mvs_mvrls, mvs_mvvgen,
  mvs_mvsample, mvs_mvnow, mvs_mvcat, mvs_mvseq, mvs_mvtx, mvs_outer,
  mvs_inner, mvs_mvv, mvs_mvp, mvs_mvtrunc, mvs_r1, mvs_r2, mvs_r3,
  mvs_sublink, mvs_cte, mvs_from, mvs_nowm, mvs_mv;
DROP VIEW mvs_v;
DROP SEQUENCE mvs_seq;
DROP TABLE mvs_unlogged, mvs_vac, mvs_conc, mvs_srf_src, mvs_rls, mvs_vgen, mvs_bump_a, mvs_tx,
  mvs_inner_src, mvs_vbase, mvs_p, mvs_trunc, mvs_shared, mvs_sub, mvs_t;
