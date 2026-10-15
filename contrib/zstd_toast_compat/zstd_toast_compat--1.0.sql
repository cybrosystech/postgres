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

-- Force the shared library to load now, in this session, so a DBA running
-- CREATE EXTENSION to unblock a specific case sees it take effect
-- immediately rather than only on the next new connection.
SELECT zstd_toast_compat_enable();
