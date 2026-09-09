/* contrib/dbblue_columnar/dbblue_columnar--1.2--1.3.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_columnar UPDATE TO '1.3'" to load this file. \quit

-- 1.3: per-database isolation of the column store budget.
--
-- The store is one cluster-wide shared area, but until now its memory was
-- accounted by a single counter. On a server hosting several databases (the
-- normal case for Odoo) that meant they silently competed: whichever database
-- populated first took what it wanted, and the others got a degraded store
-- whose only symptom was scans quietly falling back to heap. Tuning the engine
-- for one tenant was therefore not isolated from the rest.
--
-- Now dbblue_columnar.memory_mb is the CLUSTER ceiling and the new
-- dbblue_columnar.database_memory_mb is one database's quota within it, set
-- per tenant with ALTER DATABASE. Alongside it, dbblue_columnar.enabled,
-- .naptime, .refresh_threshold and .auto_columnarize became PGC_SUSET, so all
-- of them are settable per database too.

-- This database's share of the budget, plus the three denial counters that
-- distinguish why a store stopped growing (they have opposite fixes).
CREATE FUNCTION dbblue_columnar_database_memory(
	OUT database_quota_mb integer,
	OUT database_used_bytes bigint,
	OUT cluster_budget_mb integer,
	OUT cluster_used_bytes bigint,
	OUT denials_db_quota bigint,
	OUT denials_cluster bigint,
	OUT denials_no_slot bigint,
	OUT databases_tracked integer,
	OUT max_databases integer)
RETURNS record
AS 'MODULE_PATHNAME', 'dbblue_columnar_database_memory'
LANGUAGE C VOLATILE;

-- Readable per-database budget view. Answers "is this tenant limited by its
-- own quota, or by the server being full, or not limited at all?" - which the
-- raw byte counters alone cannot.
CREATE VIEW dbblue_columnar_database_memory_status AS
	SELECT d.database_quota_mb,
	       d.database_used_bytes,
	       pg_catalog.pg_size_pretty(d.database_used_bytes) AS database_used,
	       CASE WHEN d.database_quota_mb > 0
	            THEN round(100.0 * d.database_used_bytes
	                       / (d.database_quota_mb::numeric * 1024 * 1024), 1)
	       END									AS pct_of_database_quota,
	       d.cluster_budget_mb,
	       d.cluster_used_bytes,
	       pg_catalog.pg_size_pretty(d.cluster_used_bytes) AS cluster_used,
	       round(100.0 * d.cluster_used_bytes
	             / (d.cluster_budget_mb::numeric * 1024 * 1024), 1) AS pct_of_cluster,
	       d.databases_tracked,
	       d.max_databases,
	       d.denials_db_quota,
	       d.denials_cluster,
	       d.denials_no_slot,
	       -- Ordered most-recent-cause first, and a quota verdict is only
	       -- reachable when a quota actually exists: reporting "hit its quota"
	       -- for a database whose quota is 0 sent an operator to raise a limit
	       -- that was never enforced.
	       CASE
	           WHEN d.denials_no_slot > 0
	               THEN 'too many databases hold a store at once (raise DBBC_MAX_DATABASES)'
	           WHEN d.denials_cluster > 0
	               THEN 'server-wide memory_mb is full - another database must give some back'
	           WHEN d.database_quota_mb > 0 AND d.denials_db_quota > 0
	               THEN 'this database hit its own database_memory_mb quota - raise it for this database'
	           WHEN d.database_quota_mb = 0
	               THEN 'no per-database quota set - this database competes with the others for memory_mb'
	           ELSE 'within budget'
	       END									AS verdict
	FROM dbblue_columnar_database_memory() d;
