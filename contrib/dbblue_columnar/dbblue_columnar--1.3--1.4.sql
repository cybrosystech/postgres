/* contrib/dbblue_columnar/dbblue_columnar--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_columnar UPDATE TO '1.4'" to load this file. \quit

-- 1.4: keep the store and the registration bookkeeping from drifting apart,
-- plus a way to see it directly if they ever do.
--
-- dbblue_columnar_relations is an ordinary heap table owned by the extension.
-- DROP EXTENSION dbblue_columnar (rather than the supported ALTER EXTENSION
-- ... UPDATE path) removes it, but has NO effect on the column store, which
-- lives in raw shared memory outside any extension's SQL objects. A
-- subsequent CREATE EXTENSION then starts with an empty registration table
-- while the old store versions sit there ORPHANED: still resident, still
-- being served (the planner looks the store up by reloid directly and never
-- consults the registration table), still charged against the memory budget,
-- but permanently invisible to dbblue_columnar_status and every other view
-- that joins off the registrations.

-- Raw introspection: one row per relation of THIS database that has a live
-- version in the shared store, read directly from the shared hash -
-- independent of dbblue_columnar_relations. Named _versions (not _status) to
-- parallel dbblue_columnar_blocks() vs dbblue_columnar_status: this is the raw
-- SRF, dbblue_columnar_store_status below is the readable view over it.
CREATE FUNCTION dbblue_columnar_store_versions(
	OUT reloid oid,
	OUT ncols integer,
	OUT nblocks bigint,
	OUT ndirslots bigint,
	OUT av_at_build bigint,
	OUT total_bytes bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'dbblue_columnar_store_status'
LANGUAGE C VOLATILE STRICT;

-- The one-query answer to "is anything orphaned right now?":
--   SELECT * FROM dbblue_columnar_store_status WHERE NOT registered;
-- relation_name is NULL for a reloid whose relation no longer exists at all
-- (DROP TABLE, not just DROP EXTENSION) - still worth seeing, since it is
-- still consuming budget.
CREATE VIEW dbblue_columnar_store_status AS
	SELECT v.reloid,
	       c.relname							AS relation_name,
	       EXISTS (SELECT 1 FROM dbblue_columnar_relations r
	               WHERE r.relid = v.reloid)	AS registered,
	       v.ncols,
	       v.nblocks,
	       v.ndirslots,
	       v.av_at_build,
	       v.total_bytes,
	       pg_catalog.pg_size_pretty(v.total_bytes) AS store_size
	FROM dbblue_columnar_store_versions() v
	LEFT JOIN pg_catalog.pg_class c ON c.oid = v.reloid
	ORDER BY v.reloid;

-- Drop every store version belonging to the current database, registered or
-- not, and return how many were dropped. Callable directly (e.g. after
-- finding orphans via dbblue_columnar_store_status), and also what a fresh
-- CREATE EXTENSION calls automatically (see dbblue_columnar--1.4.sql, NOT
-- this delta - ALTER EXTENSION ... UPDATE must never wipe a live install's
-- real, currently-registered stores).
CREATE FUNCTION dbblue_columnar_reset_database()
RETURNS integer
AS 'MODULE_PATHNAME', 'dbblue_columnar_reset_database'
LANGUAGE C VOLATILE;
