--
-- Tests for the dbblue additions in unaccent 1.2: IMMUTABLE / COST 100 and
-- unaccent_refresh().  Only ASCII is used, so no encoding check is needed.
--

-- Start from a clean slate; the "unaccent" test leaves the extension installed.
DROP EXTENSION IF EXISTS unaccent CASCADE;

-- Version 1.1 is the upstream definition: STABLE, default cost.
CREATE EXTENSION unaccent VERSION '1.1';
SELECT proname, pronargs, provolatile, procost
FROM pg_proc WHERE proname = 'unaccent' ORDER BY pronargs;
SELECT count(*) AS refresh_functions FROM pg_proc WHERE proname = 'unaccent_refresh';

-- The upgrade path must end in the same state as a fresh install.
ALTER EXTENSION unaccent UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'unaccent';
SELECT proname, pronargs, provolatile, procost, proparallel, proisstrict
FROM pg_proc WHERE proname LIKE 'unaccent%' ORDER BY proname, pronargs;
CREATE TEMP TABLE ur_upgraded AS
SELECT proname, pronargs, provolatile, procost, proparallel, proisstrict
FROM pg_proc WHERE proname LIKE 'unaccent%';

DROP EXTENSION unaccent;
CREATE EXTENSION unaccent;
SELECT extversion FROM pg_extension WHERE extname = 'unaccent';
SELECT count(*) AS differences FROM (
  (SELECT proname, pronargs, provolatile, procost, proparallel, proisstrict
   FROM pg_proc WHERE proname LIKE 'unaccent%'
   EXCEPT SELECT * FROM ur_upgraded)
  UNION ALL
  (SELECT * FROM ur_upgraded
   EXCEPT SELECT proname, pronargs, provolatile, procost, proparallel, proisstrict
   FROM pg_proc WHERE proname LIKE 'unaccent%')) AS d;

-- IMMUTABLE means an index on unaccent() is possible.
SELECT unaccent('foobar');
CREATE TABLE ur_t (id int, name text);
CREATE INDEX ur_t_idx ON ur_t (unaccent(name));
CREATE INDEX ur_t_idx2 ON ur_t (unaccent('unaccent', name));
CREATE INDEX ur_t_plain ON ur_t (name);
-- an index through an SQL wrapper is NOT found by unaccent_refresh() (known
-- limitation, see the documentation)
CREATE FUNCTION ur_wrap(text) RETURNS text LANGUAGE sql IMMUTABLE
  AS $$ SELECT public.unaccent($1) $$;
CREATE INDEX ur_t_wrap ON ur_t (ur_wrap(name));
-- stored generated columns are listed, but not rewritten
CREATE TABLE ur_g (n text,
  u text GENERATED ALWAYS AS (unaccent(n)) STORED,
  o text GENERATED ALWAYS AS (upper(n)) STORED);

-- dry run: lists the dictionary, the indexes using unaccent() and the columns
SELECT * FROM unaccent_refresh(dry_run => true);

-- a real run rebuilds exactly those indexes
CREATE TEMP TABLE ur_before AS
SELECT relname, relfilenode FROM pg_class WHERE relname LIKE 'ur_t%';
SELECT step, object, detail FROM unaccent_refresh() WHERE step <> 'column';
SELECT b.relname, b.relfilenode <> c.relfilenode AS rebuilt
FROM ur_before b JOIN pg_class c USING (relname) ORDER BY 1;

-- dict limits the dictionary reload only; every index is still covered
CREATE TEXT SEARCH DICTIONARY ur_dict (TEMPLATE = unaccent, RULES = 'unaccent');
SELECT step, object FROM unaccent_refresh(dry_run => true, dict => 'ur_dict');

-- dry run tries the reload, so a missing rules file shows up there too
SELECT step, object,
       regexp_replace(detail, '(would fail: could not open unaccent file).*', '\1') AS detail
FROM unaccent_refresh('ur_no_such_rules', dry_run => true)
WHERE step = 'dictionary';
-- ... and the real run fails the same way and changes nothing
DO $$
BEGIN
  PERFORM unaccent_refresh('ur_no_such_rules');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE '%', regexp_replace(SQLERRM, '(could not open unaccent file).*', '\1');
END $$;
SELECT dictname, dictinitoption FROM pg_ts_dict
WHERE dictname IN ('unaccent', 'ur_dict') ORDER BY dictname;

-- temporary tables of this session are rebuilt like any other
CREATE TEMP TABLE ur_tmp (name text);
CREATE INDEX ur_tmp_idx ON ur_tmp (unaccent(name));
SELECT step, regexp_replace(object, 'pg_temp_[0-9]+', 'pg_temp_N') AS object, detail
FROM unaccent_refresh() WHERE object LIKE '%ur_tmp_idx';
DROP TABLE ur_tmp;

-- A user who does not own the dictionary: the reload is skipped (not an
-- error), indexes the user may rebuild are rebuilt, the others are skipped.
CREATE ROLE regress_unaccent_user;
ALTER TABLE ur_t OWNER TO regress_unaccent_user;
CREATE TABLE ur_other (name text);
CREATE INDEX ur_other_idx ON ur_other (unaccent(name));
SET ROLE regress_unaccent_user;
SELECT step, object, detail FROM unaccent_refresh(dry_run => true) WHERE step <> 'column';
SELECT step, object, detail FROM unaccent_refresh() WHERE step <> 'column';
-- switching the rules needs ownership of the dictionary
SELECT step, object, detail FROM unaccent_refresh('unaccent', dry_run => true)
WHERE step = 'dictionary';
\set VERBOSITY terse
SELECT * FROM unaccent_refresh('unaccent');
\set VERBOSITY default
RESET ROLE;

-- cleanup
DROP TABLE ur_t, ur_other, ur_g;
DROP FUNCTION ur_wrap(text);
DROP TEXT SEARCH DICTIONARY ur_dict;
DROP ROLE regress_unaccent_user;
