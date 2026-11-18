/* contrib/dbblue_columnar/dbblue_columnar--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_columnar UPDATE TO '1.1'" to load this file. \quit

-- 1.1: richer dbblue_columnar_status view. Was just the registration list with
-- an opaque attnum; now resolves the column name and joins live per-column
-- store state (built / blocks / store_rows / store_bytes) from
-- dbblue_columnar_blocks(). DROP + CREATE because the column set changes (a
-- plain CREATE OR REPLACE cannot add columns before existing ones / reorder).
DROP VIEW dbblue_columnar_status;

CREATE VIEW dbblue_columnar_status AS
	SELECT r.relid,
	       r.attnum,
	       r.auto_added,
	       r.added_by,
	       r.added_at,
	       a.attname                  AS column_name,
	       (b.blocks IS NOT NULL)     AS built,
	       COALESCE(b.blocks, 0)      AS blocks,
	       COALESCE(b.store_rows, 0)  AS store_rows,
	       COALESCE(b.store_bytes, 0) AS store_bytes,
	       pg_catalog.pg_size_pretty(COALESCE(b.store_bytes, 0)) AS store_size
	FROM dbblue_columnar_relations r
	LEFT JOIN pg_catalog.pg_attribute a
	       ON a.attrelid = r.relid AND a.attnum = r.attnum
	LEFT JOIN LATERAL (
	       SELECT count(*)      AS blocks,
	              sum(bk.nrows) AS store_rows,
	              sum(bk.bytes) AS store_bytes
	       FROM dbblue_columnar_blocks(r.relid) bk
	       WHERE bk.attnum = r.attnum
	) b ON true
	ORDER BY r.relid, r.attnum;

-- 1.1: human-friendly memory view over dbblue_columnar_memory() (raw bytes kept,
-- pg_size_pretty'd sizes + percent-of-budget added).
CREATE VIEW dbblue_columnar_memory_status AS
	SELECT m.budget_mb,
	       m.used_bytes,
	       pg_catalog.pg_size_pretty(m.used_bytes)      AS used,
	       m.dsa_total_bytes,
	       pg_catalog.pg_size_pretty(m.dsa_total_bytes) AS dsa_total,
	       round(100.0 * m.used_bytes
	             / (m.budget_mb::numeric * 1024 * 1024), 1) AS pct_of_budget
	FROM dbblue_columnar_memory() m;
