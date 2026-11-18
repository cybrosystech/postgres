/* contrib/unaccent/unaccent--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION unaccent UPDATE TO '1.2'" to load this file. \quit

-- Dbblue: see unaccent--1.2.sql.  Existing indexes need no rebuild by this
-- change itself.  For COST 100 see unaccent--1.2.sql.
ALTER FUNCTION unaccent(regdictionary, text) IMMUTABLE COST 100;
ALTER FUNCTION unaccent(text) IMMUTABLE COST 100;

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
-- With dry_run it only reports what it would do.  dict restricts step 1 to one
-- dictionary.  Everything runs in one transaction: if any step fails, nothing
-- is changed.  REINDEX blocks writes to the tables meanwhile; for very large or
-- busy tables, run the dictionary reload and REINDEX CONCURRENTLY by hand.
-- Other sessions pick up the new rules the next time they use the dictionary.
-- Not covered: stored data derived from unaccent() (generated columns,
-- materialized view contents), which must be refreshed separately.
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

COMMENT ON FUNCTION unaccent_refresh(text, boolean, regdictionary) IS
'reload the unaccent dictionaries (optionally with new rules) and reindex every index that uses unaccent()';
