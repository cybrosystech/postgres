--
-- DBblue COUNT cache must never cache a count over a foreign table.
--
-- A foreign table's rows do not live here.  Writes go through the FDW rather
-- than the tableam wrappers where per-relation write stamps are bumped, so no
-- stamp ever moves -- and the remote side can be changed by something outside
-- this cluster entirely, which no local tracking could observe.
--
-- Aggregate pushdown normally hides this, because the plan root becomes a
-- ForeignScan and the cache's shape gate declines it for that reason.  A qual
-- the FDW cannot ship keeps the Agg local, and that is the case this checks.
--
-- Lives here rather than in core regress because it needs a working FDW.
--

-- Quiet, so the test does not depend on whether postgres_fdw.sql ran first.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
RESET client_min_messages;

CREATE TABLE dbb_fdw_base (id int);
INSERT INTO dbb_fdw_base SELECT generate_series(1, 100);

DO $$
BEGIN
    EXECUTE format(
        'CREATE SERVER dbb_loop FOREIGN DATA WRAPPER postgres_fdw '
        'OPTIONS (dbname %L, port %L)',
        current_database(), current_setting('port'));
END $$;
CREATE USER MAPPING FOR CURRENT_USER SERVER dbb_loop;
CREATE FOREIGN TABLE dbb_ft (id int)
    SERVER dbb_loop OPTIONS (table_name 'dbb_fdw_base');

-- Not inlinable, and not shippable, so the aggregate stays local.
CREATE FUNCTION dbb_local(i int) RETURNS int
    LANGUAGE plpgsql IMMUTABLE AS 'BEGIN RETURN i; END';

SET dbblue_count_cache = on;
SET dbblue_offset_flip = on;

-- Confirm the shape really is a local Agg over a ForeignScan; if this ever
-- changes to a pushed-down aggregate, the test stops covering what it claims.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM dbb_ft WHERE id > 0 AND dbb_local(id) > 0;

SELECT count(*) AS first_count FROM dbb_ft WHERE id > 0 AND dbb_local(id) > 0;

-- Change the underlying table directly, exactly as a remote change would
-- appear: no tableam write is seen for dbb_ft itself.
INSERT INTO dbb_fdw_base SELECT generate_series(101, 150);

-- Must reflect the new rows.  A cached count would still say 100.
SELECT count(*) AS after_change FROM dbb_ft WHERE id > 0 AND dbb_local(id) > 0;

RESET dbblue_count_cache;
RESET dbblue_offset_flip;

DROP FOREIGN TABLE dbb_ft;
DROP USER MAPPING FOR CURRENT_USER SERVER dbb_loop;
DROP SERVER dbb_loop;
DROP FUNCTION dbb_local(int);
DROP TABLE dbb_fdw_base;
