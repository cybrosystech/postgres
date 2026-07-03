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

-- Human-friendly view over the registrations.
CREATE VIEW dbblue_columnar_status AS
	SELECT relid, attnum, auto_added, added_by, added_at
	FROM dbblue_columnar_relations
	ORDER BY relid, attnum;
