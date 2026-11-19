--
-- DBblue raw bulk insert (dbblue_enable_tableam_multi_insert).
--
-- The fast path for populating a brand-new relation (CREATE TABLE AS, CREATE
-- MATERIALIZED VIEW) bypasses the buffer manager and assumes it is the sole
-- writer, laying pages down from block 0.  A SELECT that writes into the very
-- table being created -- a volatile function or data-modifying CTE -- is a
-- second writer that would otherwise silently overwrite rows.  The load must
-- fail cleanly instead, and (being a single transaction) leave no table behind.
--
-- A plain SELECT that does not touch its own target still takes the fast path.
--
-- The second half checks that the fast path is really taken by every caller
-- (CREATE TABLE AS, CREATE MATERIALIZED VIEW, REFRESH, REFRESH CONCURRENTLY,
-- temp and unlogged targets), and that what it writes is correct: contents
-- equal to the source across many bulk-write flushes, TOAST, fillfactor
-- packing, xmin, rollback, and indexes built on top of it.
--
-- Whether the fast path ran is read from this backend's own I/O statistics.
-- The buffer-managed insert path extends a permanent relation through a
-- BAS_BULKWRITE ring (context 'bulkwrite') and a temp relation through local
-- buffers (object 'temp relation'); the fast path writes straight to the file
-- via bulk_write.c and is counted in neither.  Catalog extends land in
-- 'relation'/'normal' and B-tree builds also bypass the buffer manager, so
-- neither disturbs the count.  Every check is paired with the same statement
-- on the slow path, which must show extends, so a probe that stopped counting
-- anything would fail the test rather than make it pass.
--
-- Crash recovery, wal_level = minimal, checkpoint during the load and
-- standby replay are in src/test/recovery/t/061_dbblue_bulk_insert_durability.pl.
-- The pages are read back
-- from disk on first access (the fast path never put them in shared buffers),
-- so with data checksums on -- the regression default -- a bad checksum
-- fails the reads below.
--
-- Self-contained: dbi_ objects.  Assumes the fast path is active, i.e. not
-- wal_level = logical (XLogLogicalInfoActive()), which the default regression
-- configuration satisfies.

SET client_min_messages = warning;
\set VERBOSITY terse

CREATE FUNCTION dbi_sneak() RETURNS int LANGUAGE plpgsql AS
  $$ BEGIN EXECUTE 'INSERT INTO dbi_t VALUES (-1, ''sneaked'')'; RETURN 0; END $$;

SET dbblue_enable_tableam_multi_insert = on;

-- A CTAS whose SELECT writes into the table being created must error rather
-- than silently lose rows.  (The WHERE calls dbi_sneak() once, on g = 1.)
CREATE TABLE dbi_t AS
  SELECT g AS id, 'ctas'::text AS src FROM generate_series(1, 1000) g
  WHERE (g <> 1) OR (dbi_sneak() = 0);

-- ... and must leave nothing behind: the whole CTAS rolled back.
SELECT count(*) FROM dbi_t;

-- A normal CTAS (the SELECT does not touch its target) still works, on the
-- fast path.
CREATE TABLE dbi_ok AS
  SELECT g AS id, 'ctas'::text AS src FROM generate_series(1, 1000) g;
SELECT count(*) FROM dbi_ok;

-- With the fast path off, the same self-writing CTAS is allowed -- both writers
-- go through the buffer manager -- and loses nothing: 1000 rows + 1 sneaked.
SET dbblue_enable_tableam_multi_insert = off;
CREATE TABLE dbi_t AS
  SELECT g AS id, 'ctas'::text AS src FROM generate_series(1, 1000) g
  WHERE (g <> 1) OR (dbi_sneak() = 0);
SELECT count(*) FROM dbi_t;

DROP TABLE dbi_t;
DROP TABLE dbi_ok;
DROP FUNCTION dbi_sneak();

--
-- Did the fast path run?
--
-- Extends done through the buffer manager by this backend.  Pending stats are
-- only visible after a flush, which pg_stat_force_next_flush() forces at the
-- end of the statement that calls it.
CREATE FUNCTION dbi_buffered_extends() RETURNS bigint LANGUAGE sql AS
  $$ SELECT coalesce(sum(extends), 0)::bigint
       FROM pg_stat_get_backend_io(pg_backend_pid())
      WHERE (object = 'relation' AND context = 'bulkwrite')
         OR object = 'temp relation' $$;

-- Source data: ~300 pages, far more than one bulk-write flush (32 pages), with
-- NULLs and several types.
CREATE TABLE dbi_src AS
  SELECT g AS id,
         CASE WHEN g % 7 = 0 THEN NULL ELSE 'row ' || g END AS txt,
         (g * 1.5)::numeric AS num,
         repeat(chr(65 + g % 26), 60) AS pad
    FROM generate_series(1, 20000) g;

SET dbblue_enable_tableam_multi_insert = on;

-- CREATE TABLE AS
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE TABLE dbi_fast AS SELECT * FROM dbi_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS ctas_fast_extends;

-- CREATE MATERIALIZED VIEW
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE MATERIALIZED VIEW dbi_mv AS SELECT * FROM dbi_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS matview_fast_extends;

-- REFRESH MATERIALIZED VIEW (loads a transient table, then swaps it in)
CREATE UNIQUE INDEX dbi_mv_id ON dbi_mv (id);
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
REFRESH MATERIALIZED VIEW dbi_mv;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS refresh_fast_extends;

-- REFRESH MATERIALIZED VIEW CONCURRENTLY (the transient table is a temp
-- table).  The source is unchanged, so the diff table stays empty and
-- contributes no extends of its own.
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
REFRESH MATERIALIZED VIEW CONCURRENTLY dbi_mv;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS refresh_concurrently_fast_extends;

-- Temp and unlogged targets
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE TEMP TABLE dbi_fast_temp AS SELECT * FROM dbi_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS temp_fast_extends;

SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE UNLOGGED TABLE dbi_fast_unlogged AS SELECT * FROM dbi_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS unlogged_fast_extends;

-- The same statements on the slow path must show extends: proves the probe
-- above is able to see a buffer-managed load.
SET dbblue_enable_tableam_multi_insert = off;

SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE TABLE dbi_slow AS SELECT * FROM dbi_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before > 0 AS ctas_slow_extends;

SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
REFRESH MATERIALIZED VIEW dbi_mv;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before > 0 AS refresh_slow_extends;

SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
REFRESH MATERIALIZED VIEW CONCURRENTLY dbi_mv;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before > 0 AS refresh_concurrently_slow_extends;

SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE TEMP TABLE dbi_slow_temp AS SELECT * FROM dbi_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before > 0 AS temp_slow_extends;

--
-- Is what the fast path wrote correct?
--
-- Contents: no row missing or extra, compared in both directions.
SELECT (SELECT count(*) FROM (TABLE dbi_src EXCEPT ALL TABLE dbi_fast) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_fast EXCEPT ALL TABLE dbi_src) d) AS extra;
SELECT (SELECT count(*) FROM (TABLE dbi_src EXCEPT ALL TABLE dbi_fast_temp) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_fast_temp EXCEPT ALL TABLE dbi_src) d) AS extra;
SELECT (SELECT count(*) FROM (TABLE dbi_src EXCEPT ALL TABLE dbi_fast_unlogged) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_fast_unlogged EXCEPT ALL TABLE dbi_src) d) AS extra;

-- Page layout: every row lands on the same TID as on the slow path, so the
-- fast path packs pages exactly as heap_insert() does.
SELECT (SELECT array_agg(ctid ORDER BY id) FROM dbi_fast) =
       (SELECT array_agg(ctid ORDER BY id) FROM dbi_slow) AS same_tids;

-- ... including with a fillfactor, which must leave the same free space.
SET dbblue_enable_tableam_multi_insert = on;
CREATE TABLE dbi_ff_fast WITH (fillfactor = 50) AS SELECT * FROM dbi_src;
SET dbblue_enable_tableam_multi_insert = off;
CREATE TABLE dbi_ff_slow WITH (fillfactor = 50) AS SELECT * FROM dbi_src;
SELECT (SELECT array_agg(ctid ORDER BY id) FROM dbi_ff_fast) =
       (SELECT array_agg(ctid ORDER BY id) FROM dbi_ff_slow) AS same_tids_fillfactor,
       (SELECT count(DISTINCT (ctid::text::point)[0]) FROM dbi_ff_fast) >
       (SELECT count(DISTINCT (ctid::text::point)[0]) FROM dbi_fast) AS fillfactor_used_more_pages;

SET dbblue_enable_tableam_multi_insert = on;

-- xmin is the loading transaction, and the rows are visible to the rest of it.
BEGIN;
CREATE TABLE dbi_xmin AS SELECT * FROM dbi_src;
SELECT count(*), bool_and(xmin = pg_current_xact_id()::xid) AS xmin_is_loader
  FROM dbi_xmin;
COMMIT;
SELECT count(*) FROM dbi_xmin;

-- An empty result writes no pages.
CREATE TABLE dbi_empty AS SELECT * FROM dbi_src WHERE false;
SELECT count(*), pg_relation_size('dbi_empty') AS size FROM dbi_empty;

-- Rollback discards the load; a savepoint rollback lets it be redone.
BEGIN;
CREATE TABLE dbi_rb AS SELECT * FROM dbi_src;
ROLLBACK;
SELECT to_regclass('dbi_rb') IS NULL AS rolled_back;
BEGIN;
SAVEPOINT s;
CREATE TABLE dbi_rb AS SELECT * FROM dbi_src;
ROLLBACK TO SAVEPOINT s;
CREATE TABLE dbi_rb AS SELECT * FROM dbi_src WHERE id <= 5000;
COMMIT;
SELECT count(*) FROM dbi_rb;

-- TOAST: out-of-line values go through the ordinary toast insert path while
-- the main fork is written raw; both halves must line up.
CREATE TABLE dbi_toast_src AS
  SELECT g AS id,
         (SELECT string_agg(md5(g::text || ':' || i::text), '')
            FROM generate_series(1, 300) i) AS big
    FROM generate_series(1, 200) g;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() AS before \gset
CREATE TABLE dbi_toast AS SELECT * FROM dbi_toast_src;
SELECT pg_stat_force_next_flush();
SELECT dbi_buffered_extends() - :before AS toast_fast_extends;
SELECT pg_relation_size(reltoastrelid) > 0 AS toasted
  FROM pg_class WHERE oid = 'dbi_toast'::regclass;
SELECT count(*), bool_and(length(big) = 9600) AS full_length
  FROM dbi_toast;
SELECT (SELECT count(*) FROM (TABLE dbi_toast_src EXCEPT ALL TABLE dbi_toast) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_toast EXCEPT ALL TABLE dbi_toast_src) d) AS extra;

-- REFRESH: the materialized view matches its query, through a sequential
-- scan and through the index rebuilt over the freshly loaded heap.
REFRESH MATERIALIZED VIEW dbi_mv;
SELECT (SELECT count(*) FROM (TABLE dbi_src EXCEPT ALL TABLE dbi_mv) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_mv EXCEPT ALL TABLE dbi_src) d) AS extra;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT txt FROM dbi_mv WHERE id = 12345;
SELECT txt FROM dbi_mv WHERE id = 12345;
SELECT count(*) FROM dbi_mv WHERE id BETWEEN 1001 AND 3000;
RESET enable_seqscan;
RESET enable_bitmapscan;

-- REFRESH CONCURRENTLY with changed source data: deletes, updates and
-- inserts all reach the materialized view.
DELETE FROM dbi_src WHERE id % 10 = 0;
UPDATE dbi_src SET txt = 'changed' WHERE id % 10 = 1;
INSERT INTO dbi_src SELECT g, 'new', 0, '' FROM generate_series(20001, 21000) g;
REFRESH MATERIALIZED VIEW CONCURRENTLY dbi_mv;
SELECT (SELECT count(*) FROM (TABLE dbi_src EXCEPT ALL TABLE dbi_mv) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_mv EXCEPT ALL TABLE dbi_src) d) AS extra;
REFRESH MATERIALIZED VIEW dbi_mv;
SELECT (SELECT count(*) FROM (TABLE dbi_src EXCEPT ALL TABLE dbi_mv) d) AS missing,
       (SELECT count(*) FROM (TABLE dbi_mv EXCEPT ALL TABLE dbi_src) d) AS extra;

-- cleanup
RESET dbblue_enable_tableam_multi_insert;
DROP MATERIALIZED VIEW dbi_mv;
DROP TABLE dbi_src, dbi_fast, dbi_slow, dbi_fast_temp, dbi_slow_temp,
  dbi_fast_unlogged, dbi_ff_fast, dbi_ff_slow, dbi_xmin, dbi_empty, dbi_rb,
  dbi_toast_src, dbi_toast;
DROP FUNCTION dbi_buffered_extends();
