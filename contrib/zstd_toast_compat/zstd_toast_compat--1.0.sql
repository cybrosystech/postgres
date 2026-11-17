/* contrib/zstd_toast_compat/zstd_toast_compat--1.0.sql */

\echo Use "CREATE EXTENSION zstd_toast_compat" to load this file. \quit

CREATE FUNCTION zstd_toast_compat_enable()
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION zstd_toast_compat_status()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Force the shared library to load now, so a DBA running CREATE EXTENSION
-- to unblock a specific case sees it take effect immediately in this
-- session.
--
-- This only affects the current session, and only for reading: until the
-- library is in shared_preload_libraries, the server refuses to create zstd
-- columns or write zstd values, since other processes could not read them.
-- For zstd support in every process, add zstd_toast_compat to
-- shared_preload_libraries and restart the server.  That is the only
-- setting that reaches background processes: autovacuum and logical
-- replication workers never load session_preload_libraries.  The library
-- warns when it is loaded outside shared_preload_libraries.
SELECT zstd_toast_compat_enable();
