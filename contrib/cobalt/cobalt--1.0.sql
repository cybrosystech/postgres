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
