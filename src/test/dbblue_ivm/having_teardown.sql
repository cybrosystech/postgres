-- DBblue IVM — HAVING matview teardown consistency.
--
-- A HAVING incremental matview is a hidden base matview (_dbblue_<oid>_base)
-- plus a user-facing filtering view.  Dropping the view must drop the base and
-- all incremental infrastructure (catalog rows + triggers); dropping the base
-- directly must be redirected to the view.
--
-- All counts are scoped to this test's own source table so the test is robust
-- to other incremental matviews coexisting in the database.
\set ON_ERROR_STOP on
\echo ''
\echo '=== DBblue IVM: HAVING teardown ==='
\echo ''

DROP VIEW IF EXISTS tdwn_mv CASCADE;
DROP TABLE IF EXISTS tdwn_s CASCADE;
CREATE TABLE tdwn_s(id serial PRIMARY KEY, g int, amt numeric);
INSERT INTO tdwn_s(g,amt) SELECT g%5, g FROM generate_series(1,100) g;

CREATE MATERIALIZED VIEW tdwn_mv
    WITH (incremental_refresh=true) AS
    SELECT g, SUM(amt) AS tot FROM tdwn_s GROUP BY g HAVING SUM(amt) > 200
WITH DATA;

-- Baseline: one base matview + view + 1 catalog row (for tdwn_s) + 4 triggers.
DO $$
DECLARE nview int; ncat int; ntrig int;
BEGIN
    SELECT count(*) INTO nview FROM pg_class WHERE relname='tdwn_mv' AND relkind='v';
    SELECT count(*) INTO ncat  FROM pg_dbblue_matview WHERE srctable='tdwn_s'::regclass;
    SELECT count(*) INTO ntrig FROM pg_trigger WHERE tgrelid='tdwn_s'::regclass AND tgname LIKE '\_\_mv\_delta\_%';
    IF nview=1 AND ncat=1 AND ntrig=4 THEN
        RAISE NOTICE 'HAVING setup: PASS (view=%,catalog=%,triggers=%)', nview,ncat,ntrig;
    ELSE
        RAISE EXCEPTION 'HAVING setup: FAIL (view=%,catalog=%,triggers=%)', nview,ncat,ntrig;
    END IF;
END $$;

-- Dropping the base matview directly must be refused (redirected to the view).
DO $$
DECLARE basename text; ok bool := false;
BEGIN
    SELECT c.relname INTO basename
      FROM pg_dbblue_matview p JOIN pg_class c ON c.oid=p.mvrelid
     WHERE p.srctable='tdwn_s'::regclass;
    BEGIN
        EXECUTE 'DROP MATERIALIZED VIEW '||quote_ident(basename);
    EXCEPTION WHEN OTHERS THEN
        ok := true;
    END;
    IF ok THEN RAISE NOTICE 'DROP base redirected: PASS';
    ELSE RAISE EXCEPTION 'DROP base redirected: FAIL (base drop was allowed, would orphan the view)'; END IF;
END $$;

-- Dropping the view must tear everything down: no orphan base, catalog, triggers.
\echo 'DROP VIEW tdwn_mv -> must remove base + catalog + triggers'
DROP VIEW tdwn_mv;

DO $$
DECLARE nview int; ncat int; ntrig int;
BEGIN
    SELECT count(*) INTO nview FROM pg_class WHERE relname='tdwn_mv';
    SELECT count(*) INTO ncat  FROM pg_dbblue_matview WHERE srctable='tdwn_s'::regclass;
    SELECT count(*) INTO ntrig FROM pg_trigger WHERE tgrelid='tdwn_s'::regclass AND tgname LIKE '\_\_mv\_delta\_%';
    IF nview=0 AND ncat=0 AND ntrig=0 THEN
        RAISE NOTICE 'DROP VIEW teardown: PASS (no orphans)';
    ELSE
        RAISE EXCEPTION 'DROP VIEW teardown: FAIL (view=%,catalog=%,triggers=%)', nview,ncat,ntrig;
    END IF;
END $$;

DROP TABLE tdwn_s;
\echo ''
\echo '=== HAVING teardown test complete ==='
