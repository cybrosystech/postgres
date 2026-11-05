/* contrib/cobalt/cobalt--1.0.sql */

\echo Use "CREATE EXTENSION cobalt" to load this file. \quit

CREATE FUNCTION cobalt_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD cobalt TYPE INDEX HANDLER cobalt_handler;
COMMENT ON ACCESS METHOD cobalt IS 'cobalt index access method: bitmap-posting trigram index';

CREATE OPERATOR CLASS cobalt_text_ops
DEFAULT FOR TYPE text USING cobalt AS
	OPERATOR	1	~~ (text, text),
	OPERATOR	2	~~* (text, text);

-- Dbblue: cobalt indexes are meant for "unaccent(col) ILIKE unaccent('%x%')",
-- which needs unaccent() to be IMMUTABLE (upstream declares it STABLE).  If the
-- unaccent extension is installed, make its functions immutable.  If the
-- dictionary rules ever change, REINDEX the indexes that use unaccent().
DO $$
DECLARE
	fn regprocedure;
	found boolean := false;
BEGIN
	FOR fn IN
		SELECT p.oid::regprocedure
		FROM pg_depend d
		JOIN pg_extension e ON e.oid = d.refobjid
		JOIN pg_proc p ON p.oid = d.objid
		WHERE d.classid = 'pg_proc'::regclass
		  AND d.refclassid = 'pg_extension'::regclass
		  AND d.deptype = 'e'
		  AND e.extname = 'unaccent'
		  AND p.proname = 'unaccent'
		  AND p.provolatile <> 'i'
	LOOP
		EXECUTE format('ALTER FUNCTION %s IMMUTABLE', fn);
		found := true;
		RAISE NOTICE 'cobalt: made % immutable', fn;
	END LOOP;

	IF NOT found AND NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'unaccent') THEN
		RAISE NOTICE 'cobalt: extension unaccent is not installed, nothing to make immutable';
	END IF;
END
$$;

-- cobalt_refresh_unaccent: repair everything after the unaccent rules change.
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
-- With dry_run it only reports what it would do.  dict restricts step 1 to one
-- dictionary.  Everything runs in one transaction: if any step fails, nothing
-- is changed.  REINDEX blocks writes to the tables meanwhile; for very large or
-- busy tables, run the dictionary reload and REINDEX CONCURRENTLY by hand.
-- Other sessions pick up the new rules the next time they use the dictionary.
-- Not covered: stored data derived from unaccent() (generated columns,
-- materialized view contents), which must be refreshed separately.
CREATE FUNCTION cobalt_refresh_unaccent(
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
	rules text;
BEGIN
	FOR d IN
		SELECT t.oid AS dictoid, n.nspname, t.dictname, t.dictinitoption
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
		ELSIF dry_run THEN
			detail := format('would reload rules ''%s''', rules);
		ELSE
			EXECUTE format('ALTER TEXT SEARCH DICTIONARY %I.%I (RULES = %L)',
						   d.nspname, d.dictname, rules);
			detail := format('reloaded rules ''%s''', rules);
		END IF;
		RETURN NEXT;
	END LOOP;

	FOR i IN
		SELECT DISTINCT n.nspname, c.relname, am.amname
		FROM pg_depend dep
		JOIN pg_proc p ON p.oid = dep.refobjid
		JOIN pg_class c ON c.oid = dep.objid
		JOIN pg_index x ON x.indexrelid = c.oid
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
		IF dry_run THEN
			detail := format('would reindex (%s)', i.amname);
		ELSE
			EXECUTE format('REINDEX INDEX %I.%I', i.nspname, i.relname);
			detail := format('reindexed (%s)', i.amname);
		END IF;
		RETURN NEXT;
	END LOOP;
END
$$;

COMMENT ON FUNCTION cobalt_refresh_unaccent(text, boolean, regdictionary) IS
'reload the unaccent dictionaries (optionally with new rules) and reindex every index that uses unaccent()';
