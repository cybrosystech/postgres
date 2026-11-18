/* contrib/dbblue_columnar/dbblue_columnar--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dbblue_columnar" to load this file. \quit

-- Registration catalog: which (relation, column) pairs are columnarized.
--
-- NB: this is an ordinary extension-owned table, not a bootstrapped system
-- catalog, so it deliberately does NOT carry the reserved "pg_" prefix. The
-- "pg_" prefix is for in-core catalogs (like the IVM engine's
-- pg_dbblue_matview); a loadable module registers its state in a regular
-- table. The column-block DATA lives only in memory and is never stored here;
-- only the registrations (configuration) are persisted and dumped.
CREATE TABLE dbblue_columnar_relations (
	relid		regclass	NOT NULL,
	attnum		smallint	NOT NULL,
	auto_added	boolean		NOT NULL DEFAULT false,
	added_by	name		NOT NULL DEFAULT current_user,
	added_at	timestamptz	NOT NULL DEFAULT now(),
	PRIMARY KEY (relid, attnum)
);

-- Persist registrations across dump/restore (but never any cache data).
SELECT pg_catalog.pg_extension_config_dump('dbblue_columnar_relations', '');

-- Register columns of a relation for columnarization.
-- Milestone 1: this only records the registration; no column store is built.
CREATE FUNCTION dbblue_columnar_add(rel regclass, columns text[])
RETURNS integer
AS 'MODULE_PATHNAME', 'dbblue_columnar_add'
LANGUAGE C VOLATILE STRICT;

-- Unregister columns of a relation (inverse of dbblue_columnar_add). Only
-- edits the registry; the change takes effect on the next
-- dbblue_columnar_populate. Returns the number of registrations removed.
CREATE FUNCTION dbblue_columnar_remove(rel regclass, columns text[])
RETURNS integer
AS 'MODULE_PATHNAME', 'dbblue_columnar_remove'
LANGUAGE C VOLATILE STRICT;

-- Human-friendly view over the registrations.
CREATE VIEW dbblue_columnar_status AS
	SELECT relid, attnum, auto_added, added_by, added_at
	FROM dbblue_columnar_relations
	ORDER BY relid, attnum;

-- Build (or rebuild) the in-memory column store for a registered relation.
-- Returns the number of columnar blocks built. Only heap-page ranges that are
-- entirely all-visible are built; the rest stay heap-only.
CREATE FUNCTION dbblue_columnar_populate(rel regclass)
RETURNS integer
AS 'MODULE_PATHNAME', 'dbblue_columnar_populate'
LANGUAGE C VOLATILE STRICT;

-- Introspection: one row per (columnar block, column chunk).
CREATE FUNCTION dbblue_columnar_blocks(rel regclass,
	OUT block_index integer,
	OUT first_page bigint,
	OUT npages integer,
	OUT nrows bigint,
	OUT attnum smallint,
	OUT encoding text,
	OUT null_count bigint,
	OUT zone_min text,
	OUT zone_max text,
	OUT bytes bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'dbblue_columnar_blocks'
LANGUAGE C VOLATILE STRICT;

-- Free the in-memory column store for a relation (works by OID even after
-- DROP TABLE). Returns whether an entry existed. Registrations are kept.
CREATE FUNCTION dbblue_columnar_drop(rel regclass)
RETURNS boolean
AS 'MODULE_PATHNAME', 'dbblue_columnar_drop'
LANGUAGE C VOLATILE STRICT;

-- Column-store memory accounting. used_bytes is the engine's logical
-- accounting; dsa_total_bytes is the real shared-area size including
-- allocator overhead.
CREATE FUNCTION dbblue_columnar_memory(
	OUT budget_mb integer,
	OUT used_bytes bigint,
	OUT dsa_total_bytes bigint)
RETURNS record
AS 'MODULE_PATHNAME', 'dbblue_columnar_memory'
LANGUAGE C VOLATILE;
