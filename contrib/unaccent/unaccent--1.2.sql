/* contrib/unaccent/unaccent--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION unaccent" to load this file. \quit

-- Dbblue: unaccent() is declared IMMUTABLE (upstream: STABLE) so that SQL
-- wrappers around it can be inlined and it can be used in index expressions.
-- Odoo makes unaccent(text) immutable itself for the same reason.  The price is
-- that PostgreSQL cannot notice a change of the rules file: after changing the
-- rules call unaccent_refresh() (below) to reload them and rebuild the indexes.
--
-- COST 100 (C default: 1) is deliberate, like to_tsvector/ts_headline in core.
-- Once an IMMUTABLE SQL wrapper around unaccent() is inlined, the planner
-- costs this C function directly; at cost 1 it treats unaccent() as nearly
-- free and prefers filtering every row over the trigram indexes, which made
-- small-table and broad-pattern searches up to 10x slower in testing.  100 is
-- also what an un-inlined SQL wrapper is costed at.  Do not lower it without
-- re-running the search benchmarks.
CREATE FUNCTION unaccent(regdictionary, text)
	RETURNS text
	AS 'MODULE_PATHNAME', 'unaccent_dict'
	LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE COST 100;

CREATE FUNCTION unaccent(text)
	RETURNS text
	AS 'MODULE_PATHNAME', 'unaccent_dict'
	LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE COST 100;

CREATE FUNCTION unaccent_init(internal)
	RETURNS internal
	AS 'MODULE_PATHNAME', 'unaccent_init'
	LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION unaccent_lexize(internal,internal,internal,internal)
	RETURNS internal
	AS 'MODULE_PATHNAME', 'unaccent_lexize'
	LANGUAGE C PARALLEL SAFE;

CREATE TEXT SEARCH TEMPLATE unaccent (
	INIT = unaccent_init,
	LEXIZE = unaccent_lexize
);

CREATE TEXT SEARCH DICTIONARY unaccent (
	TEMPLATE = unaccent,
	RULES    = 'unaccent'
);

-- unaccent_refresh: repair everything after the unaccent rules change.
--
-- unaccent() is declared IMMUTABLE, so PostgreSQL cannot notice when the rules
-- file changes (edited in place, replaced by an upgrade, or switched to another
-- file); indexes built with the old rules then silently give wrong results.
-- This function
--   1. re-reads the rules of every text search dictionary built from the
--      unaccent template, optionally switching them to new_rules (the base name
--      of a .rules file in tsearch_data, as in ALTER TEXT SEARCH DICTIONARY),
--   2. rebuilds every index whose definition uses a function named unaccent,
--      whatever its access method (btree, gin, gist, cobalt, ...).
-- With dry_run it only reports what it would do (the dictionary reload is
-- tried and undone, so a missing rules file shows up as 'would fail').  dict
-- restricts step 1 to one dictionary; step 2 always covers every index that
-- uses unaccent(), because unaccent(text) picks its dictionary implicitly and
-- an index cannot reliably be tied to one dictionary.  Everything runs in one
-- transaction: if any step fails, nothing
-- is changed.  REINDEX blocks writes to the tables meanwhile; for very large or
-- busy tables, run the dictionary reload and REINDEX CONCURRENTLY by hand.
-- Other sessions pick up the new rules the next time they use the dictionary,
-- except in plans they have already cached: because unaccent() is IMMUTABLE,
-- a call with constant arguments is folded to a constant when the statement
-- is planned (PL/pgSQL function bodies, PREPAREd statements, IMMUTABLE SQL
-- functions), and a rules change does not invalidate such plans.  Long-lived
-- sessions keep the old result until they reconnect or run DISCARD PLANS.
-- Calls on column values or parameters are evaluated at run time and are not
-- affected.
--
-- Permissions: the function runs with the caller's rights.  A dictionary the
-- caller does not own (extension objects belong to the superuser who ran
-- the install script) cannot be reloaded, and an index the caller may not
-- REINDEX cannot be rebuilt.  These are not errors: each is reported as a
-- 'skipped: ...' row (and by dry_run), and the rest still runs.  The same goes
-- for indexes on temporary tables of other sessions, which cannot be rebuilt
-- (and need no repair once that session ends); the caller's own temporary
-- tables are rebuilt as usual.
--
-- Sessions started after the rules file changed already read the new rules, so
-- a non-owner can run this from a fresh connection; an owner or superuser must
-- reload the dictionary for sessions that are already open.  Switching to
-- new_rules needs ownership and fails if the caller is not the owner.
-- Stored generated columns that use unaccent() are listed (step 'column') but
-- not rewritten, as that rewrites the whole table; the ALTER TABLE command to
-- do it is in the detail.  Not covered: materialized view contents, which must
-- be refreshed separately.
CREATE FUNCTION unaccent_refresh(
	new_rules text DEFAULT NULL,
	dry_run boolean DEFAULT false,
	dict regdictionary DEFAULT NULL)
RETURNS TABLE (step text, object text, detail text)
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
	d record;
	i record;
	g record;
	rules text;
BEGIN
	FOR d IN
		SELECT t.oid AS dictoid, n.nspname, t.dictname, t.dictinitoption,
			   pg_has_role(current_user, t.dictowner, 'USAGE') AS is_owner
		FROM pg_ts_dict t
		JOIN pg_namespace n ON n.oid = t.dictnamespace
		JOIN pg_ts_template tm ON tm.oid = t.dicttemplate
		WHERE tm.tmplname = 'unaccent'
		  AND (dict IS NULL OR t.oid = dict::oid)
		ORDER BY n.nspname, t.dictname
	LOOP
		rules := coalesce(new_rules,
						  substring(d.dictinitoption from '(?i)rules\s*=\s*''([^'']*)'''));
		step := 'dictionary';
		object := format('%I.%I', d.nspname, d.dictname);
		IF rules IS NULL THEN
			detail := 'skipped: no RULES option';
		ELSIF NOT d.is_owner THEN
			IF new_rules IS NOT NULL THEN
				IF dry_run THEN
					detail := 'would fail: not owner of dictionary, cannot switch rules';
				ELSE
					RAISE EXCEPTION 'must be owner of text search dictionary %', object
						USING ERRCODE = 'insufficient_privilege',
							  HINT = 'Run unaccent_refresh() without new_rules, or as the dictionary owner.';
				END IF;
			ELSE
				detail := 'skipped: not owner; new sessions already read the current rules file';
			END IF;
		ELSIF dry_run THEN
			-- Really try the reload, then undo it, so that a missing or broken
			-- rules file is reported now and not by the real run.
			BEGIN
				EXECUTE format('ALTER TEXT SEARCH DICTIONARY %I.%I (RULES = %L)',
							   d.nspname, d.dictname, rules);
				RAISE EXCEPTION 'dry run' USING ERRCODE = 'U0001';
			EXCEPTION
				WHEN SQLSTATE 'U0001' THEN
					detail := format('would reload rules ''%s''', rules);
				WHEN OTHERS THEN
					detail := format('would fail: %s', SQLERRM);
			END;
		ELSE
			EXECUTE format('ALTER TEXT SEARCH DICTIONARY %I.%I (RULES = %L)',
						   d.nspname, d.dictname, rules);
			detail := format('reloaded rules ''%s''', rules);
		END IF;
		RETURN NEXT;
	END LOOP;

	FOR i IN
		SELECT DISTINCT n.nspname, c.relname, am.amname,
			   (has_table_privilege(x.indrelid, 'MAINTAIN')
				OR pg_has_role(current_user, tbl.relowner, 'USAGE')) AS can_reindex,
			   pg_is_other_temp_schema(n.oid) AS other_temp
		FROM pg_depend dep
		JOIN pg_proc p ON p.oid = dep.refobjid
		JOIN pg_class c ON c.oid = dep.objid
		JOIN pg_index x ON x.indexrelid = c.oid
		JOIN pg_class tbl ON tbl.oid = x.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_am am ON am.oid = c.relam
		WHERE dep.refclassid = 'pg_proc'::regclass
		  AND dep.classid = 'pg_class'::regclass
		  AND p.proname = 'unaccent'
		  AND c.relkind = 'i'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY n.nspname, c.relname
	LOOP
		step := 'index';
		object := format('%I.%I', i.nspname, i.relname);
		IF i.other_temp THEN
			detail := format('skipped: temporary table of another session (%s)', i.amname);
		ELSIF NOT i.can_reindex THEN
			detail := format('skipped: no permission to reindex (%s)', i.amname);
		ELSIF dry_run THEN
			detail := format('would reindex (%s)', i.amname);
		ELSE
			EXECUTE format('REINDEX INDEX %I.%I', i.nspname, i.relname);
			detail := format('reindexed (%s)', i.amname);
		END IF;
		RETURN NEXT;
	END LOOP;

	-- Stored generated columns keep the values computed with the old rules.
	-- Rewriting them rewrites the whole table, so only list them.
	FOR g IN
		SELECT DISTINCT n.nspname, c.relname, a.attname
		FROM pg_depend dep
		JOIN pg_proc p ON p.oid = dep.refobjid
		JOIN pg_attrdef ad ON ad.oid = dep.objid
		JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE dep.refclassid = 'pg_proc'::regclass
		  AND dep.classid = 'pg_attrdef'::regclass
		  AND p.proname = 'unaccent'
		  AND a.attgenerated = 's'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY n.nspname, c.relname, a.attname
	LOOP
		step := 'column';
		object := format('%I.%I.%I', g.nspname, g.relname, g.attname);
		detail := 'stored generated column not refreshed: rewrite it with ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION AS (<same expression>)';
		RETURN NEXT;
	END LOOP;
END
$$;

COMMENT ON FUNCTION unaccent_refresh(text, boolean, regdictionary) IS
'reload the unaccent dictionaries (optionally with new rules) and reindex every index that uses unaccent()';
