--
-- Global partition indexes: partition DDL must be transactional for them.
--
-- A global index is one btree for all partitions of a RANGE-partitioned
-- table.  DETACH, DROP, TRUNCATE, ATTACH and heap rewrites of a partition
-- change it; each of those must leave it exactly as it was when the
-- transaction rolls back.
--

-- Compare what a forced global-index scan sees with the table's rows:
-- missing entries make via_index smaller, duplicate entries larger.
CREATE FUNCTION gpi_check(tbl regclass) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
	via_index bigint;
	via_heap bigint;
BEGIN
	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_bitmapscan = off;
	EXECUTE format('SELECT count(*) FROM %s WHERE id >= 0', tbl) INTO via_index;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_seqscan = on;
	EXECUTE format('SELECT count(*) FROM %s', tbl) INTO via_heap;
	RETURN CASE WHEN via_index = via_heap THEN 'ok ' || via_heap
		ELSE format('MISMATCH index=%s heap=%s', via_index, via_heap) END;
END $$;

CREATE FUNCTION gpi_reset() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
	SET LOCAL client_min_messages = warning;
	DROP TABLE IF EXISTS gpi, gpi_new, gpi_y2023;
	CREATE TABLE gpi (id int NOT NULL, d date NOT NULL) PARTITION BY RANGE (d);
	CREATE TABLE gpi_y2023 PARTITION OF gpi FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
	CREATE TABLE gpi_y2024 PARTITION OF gpi FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
	CREATE TABLE gpi_y2025 PARTITION OF gpi FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
	INSERT INTO gpi SELECT g, date '2023-01-01' + (g % 1095) FROM generate_series(1, 3000) g;
	CREATE UNIQUE INDEX gpi_id_key ON gpi (id);	-- auto-converted to GLOBAL
END $$;

SELECT gpi_reset();
SELECT indglobal FROM pg_index WHERE indexrelid = 'gpi_id_key'::regclass;
EXPLAIN (COSTS OFF) SELECT * FROM gpi WHERE id = 5;
SELECT gpi_check('gpi');

-- TRUNCATE of a partition, rolled back
BEGIN;
TRUNCATE gpi_y2023;
SELECT gpi_check('gpi');
ROLLBACK;
SELECT gpi_check('gpi');
SELECT count(*) FROM gpi WHERE id = 5;

-- TRUNCATE of the parent, rolled back
BEGIN;
TRUNCATE gpi;
ROLLBACK;
SELECT gpi_check('gpi');

-- DETACH, rolled back
BEGIN;
ALTER TABLE gpi DETACH PARTITION gpi_y2023;
SELECT gpi_check('gpi');
ROLLBACK;
SELECT gpi_check('gpi');

-- DROP of a partition, rolled back
BEGIN;
DROP TABLE gpi_y2023;
ROLLBACK;
SELECT gpi_check('gpi');

-- an error later in the transaction rolls the TRUNCATE back too
BEGIN;
TRUNCATE gpi_y2023;
SELECT 1 / 0;
ROLLBACK;
SELECT gpi_check('gpi');

-- ROLLBACK TO SAVEPOINT
BEGIN;
SAVEPOINT s1;
ALTER TABLE gpi DETACH PARTITION gpi_y2023;
ROLLBACK TO s1;
COMMIT;
SELECT gpi_check('gpi');

-- TRUNCATE and refill in one transaction, committed and rolled back
BEGIN;
TRUNCATE gpi_y2023;
INSERT INTO gpi SELECT g, '2023-06-06' FROM generate_series(1, 20) g;
COMMIT;
SELECT gpi_check('gpi');
SELECT gpi_reset();
BEGIN;
TRUNCATE gpi_y2023;
INSERT INTO gpi SELECT g, '2023-06-06' FROM generate_series(1, 20) g;
ROLLBACK;
SELECT gpi_check('gpi');

-- committed DDL still removes and adds the right entries
ALTER TABLE gpi DETACH PARTITION gpi_y2023;
SELECT gpi_check('gpi');
INSERT INTO gpi VALUES (5, '2024-02-02');	-- reuse id 5, now only in the detached table
ALTER TABLE gpi ATTACH PARTITION gpi_y2023
	FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');	-- fails: duplicate id 5
SELECT gpi_check('gpi');	-- the failed ATTACH left nothing behind
DELETE FROM gpi WHERE id = 5;
ALTER TABLE gpi ATTACH PARTITION gpi_y2023
	FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
SELECT gpi_check('gpi');
SET enable_seqscan = off;
SELECT count(*) FROM gpi WHERE id = 5;
RESET enable_seqscan;

-- ATTACH rolled back, then attached for real
SELECT gpi_reset();
CREATE TABLE gpi_new (id int NOT NULL, d date NOT NULL);
INSERT INTO gpi_new VALUES (9001, '2026-02-01');
BEGIN;
ALTER TABLE gpi ATTACH PARTITION gpi_new FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ROLLBACK;
SELECT gpi_check('gpi');
INSERT INTO gpi VALUES (9001, '2025-05-05');	-- allowed: gpi_new is not a partition
DELETE FROM gpi WHERE id = 9001;
ALTER TABLE gpi ATTACH PARTITION gpi_new FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
SELECT gpi_check('gpi');
SET enable_seqscan = off;
SELECT count(*) FROM gpi WHERE id = 9001;
RESET enable_seqscan;

-- heap rewrites, rolled back and committed
SELECT gpi_reset();
BEGIN;
ALTER TABLE gpi ADD COLUMN r float8 DEFAULT random();
ROLLBACK;
SELECT gpi_check('gpi');
CREATE INDEX gpi_y2023_d ON gpi_y2023 (d);
BEGIN;
CLUSTER gpi_y2023 USING gpi_y2023_d;
ROLLBACK;
SELECT gpi_check('gpi');
BEGIN;
ALTER TABLE gpi ALTER COLUMN id TYPE bigint;
ROLLBACK;
SELECT gpi_check('gpi');
ALTER TABLE gpi ALTER COLUMN id TYPE bigint;
SELECT indglobal FROM pg_index WHERE indexrelid = 'gpi_id_key'::regclass;
SELECT gpi_check('gpi');
-- a type change that creates duplicates across partitions is rejected
INSERT INTO gpi VALUES (1000001, '2023-03-03'), (2000001, '2025-03-03');
ALTER TABLE gpi ALTER COLUMN id TYPE int USING (id % 1000000)::int;
SELECT gpi_check('gpi');

-- a type change that needs no rewrite keeps the global index's storage
-- (CheckIndexCompatible must ignore its trailing partition key column)
SELECT gpi_reset();
ALTER TABLE gpi ADD COLUMN code varchar(50);
UPDATE gpi SET code = 'C' || id;
CREATE UNIQUE INDEX gpi_code_key ON gpi (code);
SELECT relfilenode AS code_key_file FROM pg_class WHERE relname = 'gpi_code_key' \gset
ALTER TABLE gpi ALTER COLUMN code TYPE varchar(80);
SELECT relfilenode = :code_key_file AS storage_reused, indglobal
FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid WHERE relname = 'gpi_code_key';
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF) SELECT count(*) FROM gpi WHERE code >= '';
SELECT count(*) FROM gpi WHERE code >= '';
RESET enable_seqscan;
RESET enable_bitmapscan;
INSERT INTO gpi VALUES (99001, '2025-06-06', 'C10');	-- duplicate code
ALTER TABLE gpi ALTER COLUMN code TYPE text;
SELECT gpi_check('gpi');

-- CREATE TABLE ... PARTITION OF must not adopt stale entries: a deleted, not
-- yet vacuumed row of the DEFAULT partition leaves an entry that would route
-- to a partition created later for its range and match the new partition's
-- row at the same TID (a range scan returned it twice)
CREATE TABLE gpi_s (id int NOT NULL, k text NOT NULL, d date NOT NULL) PARTITION BY RANGE (d);
CREATE TABLE gpi_s_y2023 PARTITION OF gpi_s FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE gpi_s_def PARTITION OF gpi_s DEFAULT WITH (autovacuum_enabled = off);
SET client_min_messages = warning;
CREATE UNIQUE INDEX gpi_s_k_key ON gpi_s (k);
RESET client_min_messages;
INSERT INTO gpi_s VALUES (1, 'X', '2024-06-01');	-- goes to DEFAULT
DELETE FROM gpi_s WHERE k = 'X';
CREATE TABLE gpi_s_y2024 PARTITION OF gpi_s FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
INSERT INTO gpi_s VALUES (2, 'Y', '2024-03-03');	-- first row of the new partition
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT k, tableoid::regclass FROM gpi_s WHERE k >= 'X';
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE gpi_s;

-- INSERT ... ON CONFLICT with a global index as arbiter: the conflicting row
-- can be in another partition than the one the new row is routed to
SELECT gpi_reset();
CREATE TABLE gpi_wv (token text NOT NULL, visits int DEFAULT 1, seen date,
	d date NOT NULL) PARTITION BY RANGE (d);
CREATE TABLE gpi_wv_y2023 PARTITION OF gpi_wv FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE gpi_wv_y2024 PARTITION OF gpi_wv FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
SET client_min_messages = warning;
ALTER TABLE gpi_wv ADD CONSTRAINT gpi_wv_token_key UNIQUE (token);
RESET client_min_messages;
INSERT INTO gpi_wv VALUES ('a', 1, '2023-02-01', '2023-02-01');
-- DO UPDATE: the existing 2023 row is updated in place, nothing is inserted
INSERT INTO gpi_wv (token, seen, d) VALUES ('a', '2024-06-01', '2024-06-01')
	ON CONFLICT (token) DO UPDATE SET visits = gpi_wv.visits + 1, seen = excluded.seen
	RETURNING token, visits, seen, tableoid::regclass;
INSERT INTO gpi_wv (token, d) VALUES ('a', '2024-07-01')
	ON CONFLICT ON CONSTRAINT gpi_wv_token_key DO UPDATE SET visits = gpi_wv.visits + 1
	RETURNING visits, tableoid::regclass;
INSERT INTO gpi_wv (token, d) VALUES ('a', '2024-07-01')
	ON CONFLICT (token) DO UPDATE SET visits = 0 WHERE gpi_wv.visits > 10
	RETURNING visits;
-- moving the row to another partition is refused, as with local arbiters
INSERT INTO gpi_wv (token, d) VALUES ('a', '2024-07-01')
	ON CONFLICT (token) DO UPDATE SET d = '2024-08-01';
INSERT INTO gpi_wv (token, d) VALUES ('b', '2023-03-01'), ('b', '2024-03-01')
	ON CONFLICT (token) DO UPDATE SET visits = gpi_wv.visits + 1;
-- DO NOTHING, with and without a target, and DO SELECT
INSERT INTO gpi_wv (token, d) VALUES ('a', '2024-09-01'), ('c', '2024-09-01')
	ON CONFLICT (token) DO NOTHING RETURNING token;
INSERT INTO gpi_wv (token, d) VALUES ('a', '2024-09-01') ON CONFLICT DO NOTHING;
INSERT INTO gpi_wv (token, d) VALUES ('a', '2024-10-01')
	ON CONFLICT (token) DO SELECT RETURNING token, visits, tableoid::regclass;
SELECT token, visits, seen, d, tableoid::regclass FROM gpi_wv ORDER BY token;
DROP TABLE gpi_wv;

DROP TABLE gpi;
DROP FUNCTION gpi_check(regclass);
DROP FUNCTION gpi_reset();
