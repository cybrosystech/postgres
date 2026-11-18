/* contrib/dbblue_columnar/dbblue_columnar--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_columnar UPDATE TO '1.2'" to load this file. \quit

-- 1.2: the workload advisor.
--
-- Up to now, finding the right columns to columnarize meant guessing, then
-- discovering the answer one table at a time from the
-- dbblue_columnar.log_coverage_misses log line. The advisor turns that around:
-- two planner hooks record which columns each query shape actually reads, and
-- a flush lands the evidence here for an admin to review.
--
-- The unit of advice is a BUNDLE - one (relation, query shape) pair with the
-- complete set of columns that shape touches. It has to be atomic: a columnar
-- scan is disqualified when ANY column it references is unregistered, so
-- registering 8 of a shape's 9 columns buys exactly zero speedup. Storing
-- advice per column would invite precisely that mistake, so per-column detail
-- hangs off the bundle rather than standing on its own.

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
