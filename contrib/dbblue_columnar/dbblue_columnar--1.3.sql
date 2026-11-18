/* contrib/dbblue_columnar/dbblue_columnar--1.3.sql */

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

-- Human-friendly status view. Resolves the column name (attnum is opaque) and
-- joins live per-column store state from dbblue_columnar_blocks(): whether the
-- store is built, how many columnar blocks, the stored row count, and the
-- stored byte size (which reveals the codec effect - dict-compressed FK/enum
-- columns are far smaller than PLAIN high-cardinality ones). Registered-but-
-- unbuilt columns still appear (LEFT JOINs), with built=false and zeroed state.
-- Defined after dbblue_columnar_blocks() because it references it.
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

-- Human-friendly memory view over dbblue_columnar_memory(): keeps the raw byte
-- counts (for precision / arithmetic) and adds pg_size_pretty'd sizes plus
-- percent-of-budget, so `SELECT * FROM dbblue_columnar_memory_status` is
-- readable at a glance.
CREATE VIEW dbblue_columnar_memory_status AS
	SELECT m.budget_mb,
	       m.used_bytes,
	       pg_catalog.pg_size_pretty(m.used_bytes)      AS used,
	       m.dsa_total_bytes,
	       pg_catalog.pg_size_pretty(m.dsa_total_bytes) AS dsa_total,
	       round(100.0 * m.used_bytes
	             / (m.budget_mb::numeric * 1024 * 1024), 1) AS pct_of_budget
	FROM dbblue_columnar_memory() m;


-- ---------------------------------------------------------------------------
-- 1.2: the workload advisor.
--
-- Two planner hooks record which columns each query shape actually reads; a
-- flush lands the evidence in the tables below for an admin to review. The
-- unit of advice is a BUNDLE - one (relation, query shape) pair with the
-- complete set of columns that shape touches - because a columnar scan is
-- disqualified when ANY column it references is unregistered, so applying part
-- of a bundle buys exactly zero speedup.
-- ---------------------------------------------------------------------------

-- One row per (relation, query shape). Raw sums, not averages: the flush
-- replaces them from a running total, so the numbers are the same whatever
-- cadence it runs at.
CREATE TABLE dbblue_columnar_suggestions (
	relid			regclass	NOT NULL,
	queryid			bigint		NOT NULL,

	-- Plans that contributed measurements, and how many of those were read
	-- under a grouped aggregate. Only the scan stage contributes measurements,
	-- so nplans is the correct divisor for every *_sum column below.
	nplans			bigint		NOT NULL,
	nagg			integer		NOT NULL,

	rows_sum		bigint		NOT NULL,	-- Sum of the post-filter row estimate
	tuples_sum		bigint		NOT NULL,	-- Sum of the table's cardinality
	pages_sum		bigint		NOT NULL,
	width_sum		bigint		NOT NULL,	-- Sum of the bundle's projected width

	-- Fraction of the relation's pages that were all-visible when last seen.
	-- Only fully all-visible page ranges are ever built, so this is the
	-- ceiling on how much of the table the store could serve. NB it is
	-- page-granular and therefore an OVER-estimate of block-granular
	-- buildability: one non-all-visible page disqualifies its whole 32-page
	-- block.
	allvisfrac		double precision,

	nquals			integer		NOT NULL,	-- restriction clauses last seen
	att_overflow	boolean		NOT NULL,	-- referenced an attnum past the bitmap
	has_rls			boolean		NOT NULL,	-- a qual carried a security level

	-- 'eligible', or why this shape can never be accelerated however many
	-- columns are registered (system column, inheritance parent, non-heap AM,
	-- TABLESAMPLE, lateral dependency, ...).
	eligibility		text		NOT NULL,

	first_seen		timestamptz	NOT NULL,
	last_seen		timestamptz	NOT NULL,

	PRIMARY KEY (relid, queryid)
);

-- The member columns of a bundle, with the per-column detail a bare list of
-- attnums could not carry.
CREATE TABLE dbblue_columnar_suggestion_columns (
	relid			regclass	NOT NULL,
	queryid			bigint		NOT NULL,
	attnum			smallint	NOT NULL,

	-- Read by a restriction clause, not merely projected. Worth more: these
	-- are the columns that make zone-map block skipping work.
	is_filter		boolean		NOT NULL,

	PRIMARY KEY (relid, queryid, attnum),
	FOREIGN KEY (relid, queryid)
		REFERENCES dbblue_columnar_suggestions (relid, queryid)
		ON DELETE CASCADE
);

-- Deliberately NOT registered with pg_extension_config_dump, unlike
-- dbblue_columnar_relations. Registrations are configuration an admin chose;
-- suggestions are derived observations of one server's workload, in the same
-- category as the column blocks themselves. Restoring another host's advice
-- into a fresh database would present evidence that was never earned there.

-- Drain this database's accumulated evidence into the tables above and return
-- the number of bundles written. Ordinarily the background worker calls this
-- every dbblue_columnar.naptime seconds, but only when it has a database to
-- connect to (dbblue_columnar.autorefresh_database); calling it directly works
-- on any install and is also what makes the advisor testable.
CREATE FUNCTION dbblue_columnar_suggestions_flush()
RETURNS bigint
AS 'MODULE_PATHNAME', 'dbblue_columnar_suggestions_flush'
LANGUAGE C VOLATILE;

-- Collector health. Exists so an empty suggestion list is always diagnosable
-- rather than an unfalsifiable silence: no observations means the hooks never
-- saw a qualifying relation, noqueryid_drops means compute_query_id produced
-- no fingerprint, and nonzero contended/full drops mean
-- dbblue_columnar.suggestion_slots is too small for the workload.
CREATE FUNCTION dbblue_columnar_suggestion_stats(
	OUT collecting boolean,
	OUT slots integer,
	OUT slots_used integer,
	OUT observations bigint,
	OUT recorded bigint,
	OUT noqueryid_drops bigint,
	OUT contended_drops bigint,
	OUT full_drops bigint,
	OUT last_reset timestamptz)
RETURNS record
AS 'MODULE_PATHNAME', 'dbblue_columnar_suggestion_stats'
LANGUAGE C VOLATILE;

-- The admin-facing view: what to columnarize, best first.
--
-- The score lives HERE rather than in a stored column on purpose. Its weights
-- are still being calibrated against real ERP workloads, and keeping it in a
-- view means retuning it is a CREATE OR REPLACE VIEW, not a catalog migration
-- and a re-earning of every observation.
--
-- Shape of the score: benefit per byte of store budget.
--   * rows actually scanned per plan, times the number of plans - the work the
--     store would displace;
--   * weighted up when the shape was seen under a grouped aggregate (nagg),
--     since aggregate pushdown is a far larger win than row-serve;
--   * weighted up by selectivity, so an indexed point lookup on a huge table
--     scores near zero however often it runs;
--   * multiplied by allvisfrac, because a churning table's blocks are never
--     servable no matter how attractive the shape is - this is the term that
--     keeps hot write-heavy tables (mail_message, stock_move) off the list;
--   * divided by the projected footprint, because the store is budgeted.
--
-- Deliberately NOT joined to pg_stat_statements: that extension may not be
-- installed, and a view over a missing relation would fail at creation and
-- take ALTER EXTENSION UPDATE down with it. Join queryid yourself when you
-- have it:
--   SELECT v.*, q.query FROM dbblue_columnar_suggestion_status v
--     LEFT JOIN pg_stat_statements q USING (queryid);
CREATE VIEW dbblue_columnar_suggestion_status AS
	SELECT s.relid,
	       s.queryid,
	       s.eligibility,

	       -- what to do about it
	       c.columns,
	       c.ncolumns,
	       c.filter_columns,

	       -- how much it is worth
	       round((s.nplans
	              * (s.rows_sum::numeric / greatest(s.nplans, 1))
	              * CASE WHEN s.nagg > 0 THEN 4.0 ELSE 1.0 END
	              * CASE WHEN s.tuples_sum > 0
	                     THEN least(s.rows_sum::numeric
	                                / s.tuples_sum::numeric, 1.0)
	                     ELSE 0.0 END
	              * COALESCE(s.allvisfrac, 0.0)::numeric
	             ) / greatest(s.width_sum::numeric
	                          / greatest(s.nplans, 1)
	                          * greatest(s.tuples_sum / greatest(s.nplans, 1), 1),
	                          1.0), 2)		AS score,

	       -- why
	       s.nplans,
	       (s.nagg > 0)					AS aggregate_shape,
	       CASE WHEN s.tuples_sum > 0
	            THEN round(least(s.rows_sum::numeric
	                             / s.tuples_sum::numeric, 1.0), 4)
	            ELSE NULL END			AS selectivity,
	       s.allvisfrac,
	       s.nquals,

	       -- what it costs: predicted store footprint for the whole bundle
	       (s.width_sum / greatest(s.nplans, 1))
	           * greatest(s.tuples_sum / greatest(s.nplans, 1), 0)	AS projected_bytes,
	       pg_catalog.pg_size_pretty(
	           ((s.width_sum / greatest(s.nplans, 1))
	            * greatest(s.tuples_sum / greatest(s.nplans, 1), 0))::bigint
	       )								AS projected_size,

	       s.att_overflow,
	       s.has_rls,
	       s.first_seen,
	       s.last_seen
	FROM dbblue_columnar_suggestions s
	LEFT JOIN LATERAL (
	       SELECT array_agg(a.attname ORDER BY sc.attnum)		AS columns,
	              count(*)											AS ncolumns,
	              array_agg(a.attname ORDER BY sc.attnum)
	                  FILTER (WHERE sc.is_filter)				AS filter_columns
	       FROM dbblue_columnar_suggestion_columns sc
	       JOIN pg_catalog.pg_attribute a
	            ON a.attrelid = sc.relid AND a.attnum = sc.attnum
	           AND NOT a.attisdropped
	       WHERE sc.relid = s.relid AND sc.queryid = s.queryid
	) c ON true
	ORDER BY score DESC NULLS LAST;


-- ---------------------------------------------------------------------------
-- 1.3: per-database isolation of the column store budget.
--
-- dbblue_columnar.memory_mb is the CLUSTER ceiling; the new
-- dbblue_columnar.database_memory_mb is one database's quota within it, set
-- per tenant with ALTER DATABASE. Without the split, databases on one server
-- silently competed for a single counter and tuning one tenant degraded the
-- others. enabled/naptime/refresh_threshold/auto_columnarize are PGC_SUSET
-- for the same reason.
-- ---------------------------------------------------------------------------

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
