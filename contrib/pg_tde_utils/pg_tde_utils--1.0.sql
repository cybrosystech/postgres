/* contrib/pg_tde_utils/pg_tde_utils--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_tde_utils" to load this file. \quit

CREATE FUNCTION pg_tde_is_enabled()
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_tde_is_enabled'
LANGUAGE C STRICT;

CREATE FUNCTION pg_tde_cipher()
RETURNS text
AS 'MODULE_PATHNAME', 'pg_tde_cipher'
LANGUAGE C STRICT;

CREATE FUNCTION pg_tde_check_kmgr_file()
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_tde_check_kmgr_file'
LANGUAGE C STRICT;

CREATE FUNCTION pg_tde_rotate_cluster_key()
RETURNS boolean
AS 'MODULE_PATHNAME', 'pg_tde_rotate_cluster_key'
LANGUAGE C STRICT;

-- key management is superuser-only
REVOKE ALL ON FUNCTION pg_tde_check_kmgr_file() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_tde_rotate_cluster_key() FROM PUBLIC;
