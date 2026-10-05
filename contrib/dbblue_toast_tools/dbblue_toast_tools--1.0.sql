/* contrib/dbblue_toast_tools/dbblue_toast_tools--1.0.sql */

\echo Use "CREATE EXTENSION dbblue_toast_tools" to load this file. \quit

CREATE FUNCTION dbblue_force_detoast(anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION dbblue_force_detoast(anyelement) IS
'Return the argument as a fresh plain datum, so that re-toasting recompresses it.';

--
-- Every varlena column with storage, as a starting point for the scans below.
--
CREATE VIEW dbblue_toastable_columns AS
SELECT n.nspname::text AS schemaname,
       c.relname::text AS tablename,
       a.attname::text AS columnname,
       c.oid            AS reloid,
       a.attnum,
       c.relkind,
       c.relispartition,
       a.attcompression,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
 WHERE c.relkind IN ('r', 'm')
   AND c.relpersistence <> 't'
   AND a.attnum > 0
   AND NOT a.attisdropped
   AND a.attlen = -1
   AND n.nspname <> 'pg_toast';

COMMENT ON VIEW dbblue_toastable_columns IS
'Columns that can hold TOASTed values, including system catalogs.';

--
-- The set of relations a call applies to: everything when relation is NULL,
-- otherwise that relation plus, if it is partitioned, its whole tree.
--
CREATE FUNCTION dbblue_toast_targets(relation regclass)
RETURNS oid[]
LANGUAGE sql STABLE
AS $$
    SELECT CASE WHEN relation IS NULL THEN NULL ELSE
        (SELECT array_agg(DISTINCT o)
           FROM (SELECT relation::oid AS o
                  UNION
                 SELECT p.relid FROM pg_catalog.pg_partition_tree(relation) p
                  WHERE p.relid IS NOT NULL) s)
    END
$$;

--
-- Full audit: how many stored values use each compression method.
--
-- Scans every table, so it is not cheap; use dbblue_toast_check() when the
-- question is only whether any zstd remains.
--
CREATE FUNCTION dbblue_toast_audit(relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, columnname text,
               compression text, nvalues bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    col     record;
    targets oid[] := dbblue_toast_targets(relation);
BEGIN
    FOR col IN SELECT * FROM dbblue_toastable_columns c
                WHERE targets IS NULL OR c.reloid = ANY(targets)
                ORDER BY 1, 2, 5
    LOOP
        RETURN QUERY EXECUTE format(
            'SELECT %L::text, %L::text, %L::text,
                    coalesce(pg_catalog.pg_column_compression(%I), ''<uncompressed>'')::text,
                    count(*)::bigint
               FROM ONLY %s
              WHERE %I IS NOT NULL
              GROUP BY 4',
            col.schemaname, col.tablename, col.columnname,
            col.columnname, col.reloid::regclass, col.columnname);
    END LOOP;
END;
$$;

COMMENT ON FUNCTION dbblue_toast_audit(regclass) IS
'Count stored values per compression method, for every TOASTable column.';

--
-- Fast check: which columns still hold at least one zstd value.
--
-- Stops at the first match per column, so it is much cheaper than a full
-- audit.  Also reports columns merely *marked* zstd, since writing to such a
-- column on a server without zstd fails even if no value is compressed yet.
--
CREATE FUNCTION dbblue_toast_check(relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, columnname text, reason text)
LANGUAGE plpgsql
AS $$
DECLARE
    col     record;
    found   boolean;
    targets oid[] := dbblue_toast_targets(relation);
BEGIN
    FOR col IN SELECT * FROM dbblue_toastable_columns c
                WHERE targets IS NULL OR c.reloid = ANY(targets)
                ORDER BY 1, 2, 5
    LOOP
        IF col.attcompression = 'z' THEN
            schemaname := col.schemaname; tablename := col.tablename;
            columnname := col.columnname; reason := 'column is marked zstd';
            RETURN NEXT;
        END IF;

        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM ONLY %s
                             WHERE pg_catalog.pg_column_compression(%I) = ''zstd'')',
            col.reloid::regclass, col.columnname)
          INTO found;

        IF found THEN
            schemaname := col.schemaname; tablename := col.tablename;
            columnname := col.columnname; reason := 'contains zstd values';
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION dbblue_toast_check(regclass) IS
'List columns that still hold, or are marked for, zstd compression.';

--
-- Rewrite every zstd value to another compression method.
--
-- Defaults to a dry run: pass dry_run := false to actually apply.  Each
-- rewritten table is locked with ACCESS EXCLUSIVE for the duration of its
-- rewrite, so this belongs in a maintenance window.
--
CREATE FUNCTION dbblue_toast_normalize(target text DEFAULT 'pglz',
                                       dry_run boolean DEFAULT true,
                                       relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, columnname text,
               method text, command text)
LANGUAGE plpgsql
AS $$
DECLARE
    col     record;
    stmt    text;
    has_dep boolean;
BEGIN
    IF target NOT IN ('pglz', 'lz4') THEN
        RAISE EXCEPTION 'target must be pglz or lz4, not %', target
          USING HINT = 'Normalizing to zstd would defeat the purpose.';
    END IF;

    IF current_setting('default_toast_compression') = 'zstd' THEN
        RAISE EXCEPTION 'default_toast_compression is still zstd'
          USING HINT = 'Set it to ' || target || ' first, otherwise rewritten '
                       'values whose column uses the default would just be '
                       'recompressed with zstd again.';
    END IF;

    IF relation IS NOT NULL
       AND EXISTS (SELECT 1 FROM pg_catalog.pg_class
                    WHERE oid = relation AND relispartition) THEN
        RAISE NOTICE 'relation % is a partition; the whole partition tree will be rewritten',
                     relation;
    END IF;

    /*
     * A partition cannot have its column type altered directly, so the
     * rewrite is driven from the root of the partition tree -- which covers
     * every partition under it.  Hence the DISTINCT: without it a tree with
     * N partitions would be rewritten N times.
     */
    FOR col IN
        SELECT DISTINCT ON (s.tgt, s.columnname)
               s.tgt, s.columnname, s.coltype, s.relkind, s.attcompression,
               n.nspname::text AS tgtschema, r.relname::text AS tgtname
          FROM (SELECT c.columnname, c.coltype, c.attcompression, c.relkind,
                       CASE WHEN c.relispartition
                            THEN pg_catalog.pg_partition_root(c.reloid)
                            ELSE c.reloid::regclass END AS tgt
                  FROM dbblue_toastable_columns c
                 WHERE (c.schemaname, c.tablename, c.columnname) IN
                       (SELECT k.schemaname, k.tablename, k.columnname
                          FROM dbblue_toast_check(relation) k)) s
          JOIN pg_catalog.pg_class r ON r.oid = s.tgt
          JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
         ORDER BY s.tgt, s.columnname
    LOOP
        /*
         * ALTER COLUMN TYPE is refused outright when a view, matview or rule
         * depends on the column.  Those tables fall back to an UPDATE, which
         * has no such restriction and takes a weaker lock -- at the cost of
         * dead tuples to vacuum afterwards, and of firing row triggers, which
         * is why it is not the default and is reported in "method".
         */
        SELECT EXISTS (SELECT 1
                         FROM pg_catalog.pg_depend d
                        WHERE d.classid = 'pg_rewrite'::regclass
                          AND d.refclassid = 'pg_class'::regclass
                          AND d.refobjid = col.tgt
                          AND d.refobjsubid = (SELECT a.attnum
                                                 FROM pg_catalog.pg_attribute a
                                                WHERE a.attrelid = col.tgt
                                                  AND a.attname = col.columnname))
          INTO has_dep;

        stmt := '';

        /*
         * Only override an explicit zstd marking; a column left on the
         * default is already covered by default_toast_compression, which we
         * checked above is no longer zstd.
         */
        IF col.attcompression = 'z' THEN
            stmt := format('ALTER %s %I.%I ALTER COLUMN %I SET COMPRESSION %s; ',
                           CASE WHEN col.relkind = 'm' THEN 'MATERIALIZED VIEW'
                                ELSE 'TABLE' END,
                           col.tgtschema, col.tgtname, col.columnname, target);
        END IF;

        IF col.relkind = 'm' THEN
            /* matviews cannot ALTER COLUMN TYPE; refreshing rebuilds the data */
            method := 'refresh';
            stmt := stmt || format('REFRESH MATERIALIZED VIEW %I.%I;',
                                   col.tgtschema, col.tgtname);
        ELSIF has_dep THEN
            method := 'update';
            stmt := stmt || format(
                'UPDATE %I.%I SET %I = dbblue_force_detoast(%I);',
                col.tgtschema, col.tgtname, col.columnname, col.columnname);
        ELSE
            method := 'rewrite';
            stmt := stmt || format(
                'ALTER TABLE %I.%I ALTER COLUMN %I TYPE %s USING dbblue_force_detoast(%I);',
                col.tgtschema, col.tgtname, col.columnname,
                col.coltype, col.columnname);
        END IF;

        IF NOT dry_run THEN
            EXECUTE stmt;
        END IF;

        schemaname := col.tgtschema; tablename := col.tgtname;
        columnname := col.columnname; command := stmt;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION dbblue_toast_normalize(text, boolean, regclass) IS
'Rewrite zstd-compressed values to pglz or lz4, optionally for one relation. Dry run unless dry_run := false.';
