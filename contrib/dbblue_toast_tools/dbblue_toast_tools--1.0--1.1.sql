/* contrib/dbblue_toast_tools/dbblue_toast_tools--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION dbblue_toast_tools UPDATE TO '1.1'" to load this file. \quit

--
-- 1.1 finds zstd data that 1.0 missed:
--
--  * fields of composite values (and of composites inside arrays), which
--    keep their own compression and are invisible to pg_column_compression()
--    on the column;
--  * index entries, which are compressed independently of the table;
--  * materialized views, whose REFRESH copies already-compressed values
--    from their source tables instead of recompressing them.
--

--
-- Which relations are scanned:
--
--  * System catalogs are skipped.  The server never compresses them with
--    zstd (toast_resolve_compression()), and scanning them only made the
--    tools fail for non-superusers on catalogs such as pg_authid.
--  * The current session's own temporary tables are included.  Other
--    sessions' temporary tables cannot be read, so they are skipped; they
--    disappear with their session anyway.
--
-- The views list only catalog metadata and are readable by everyone.  The
-- scans skip tables the caller may not SELECT from, with a warning saying
-- how many; normalizing a table requires owning it.
--
CREATE OR REPLACE VIEW dbblue_toastable_columns AS
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
   AND (c.relpersistence <> 't' OR c.relnamespace = pg_catalog.pg_my_temp_schema())
   AND a.attnum > 0
   AND NOT a.attisdropped
   AND a.attlen = -1
   AND n.nspname NOT IN ('pg_catalog', 'pg_toast');

COMMENT ON VIEW dbblue_toastable_columns IS
'Columns that can hold TOASTed values, except in system catalogs (never zstd) and other sessions'' temporary tables.';

GRANT SELECT ON dbblue_toastable_columns TO PUBLIC;

--
-- dbblue_force_detoast() now also flattens composite fields and array
-- elements (C change, same signature).  Two new C helpers:
--
CREATE FUNCTION dbblue_datum_has_zstd(anyelement)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT STABLE;

COMMENT ON FUNCTION dbblue_datum_has_zstd(anyelement) IS
'True if the value, or any field or element nested inside it, is compressed with zstd.';

CREATE FUNCTION dbblue_index_zstd_entries(regclass)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION dbblue_index_zstd_entries(regclass) IS
'Number of index entries containing zstd data (btree, GiST, GIN, BRIN). NULL if the access method cannot be inspected.';

--
-- Every index that has at least one variable-length column, and so may hold
-- compressed entries.
--
CREATE VIEW dbblue_toastable_indexes AS
SELECT n.nspname::text  AS schemaname,
       c.relname::text  AS tablename,
       i.relname::text  AS indexname,
       c.oid            AS reloid,
       i.oid            AS indexoid,
       am.amname::text  AS indexam
  FROM pg_catalog.pg_index x
  JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
  JOIN pg_catalog.pg_class c ON c.oid = x.indrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_am am ON am.oid = i.relam
 WHERE c.relkind IN ('r', 'm')
   AND (c.relpersistence <> 't' OR c.relnamespace = pg_catalog.pg_my_temp_schema())
   AND i.relkind = 'i'
   AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
   AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = i.oid AND a.attnum > 0 AND a.attlen = -1);

COMMENT ON VIEW dbblue_toastable_indexes IS
'Indexes with variable-length columns, which can hold compressed entries, except on system catalogs and other sessions'' temporary tables.';

GRANT SELECT ON dbblue_toastable_indexes TO PUBLIC;

--
-- Indexes holding zstd entries, or that cannot be inspected.
--
CREATE FUNCTION dbblue_toast_check_indexes(relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, indexname text,
               zstd_entries bigint, reason text)
LANGUAGE plpgsql
AS $$
DECLARE
    ix      record;
    n       bigint;
    skipped int := 0;
    targets oid[] := dbblue_toast_targets(relation);
BEGIN
    FOR ix IN SELECT * FROM dbblue_toastable_indexes t
               WHERE targets IS NULL OR t.reloid = ANY(targets)
               ORDER BY 1, 2, 3
    LOOP
        IF NOT pg_catalog.has_table_privilege(ix.reloid, 'SELECT') THEN
            skipped := skipped + 1;
            CONTINUE;
        END IF;

        n := dbblue_index_zstd_entries(ix.indexoid);

        IF n IS NULL THEN
            reason := format('index %I cannot be inspected (access method %s); rebuild it to be safe',
                             ix.indexname, ix.indexam);
        ELSIF n > 0 THEN
            reason := format('index %I contains zstd entries', ix.indexname);
        ELSE
            CONTINUE;
        END IF;

        schemaname := ix.schemaname; tablename := ix.tablename;
        indexname := ix.indexname; zstd_entries := n;
        RETURN NEXT;
    END LOOP;

    IF skipped > 0 THEN
        RAISE WARNING 'skipped % index(es) on tables you do not have SELECT privilege on', skipped
          USING HINT = 'Run this as a superuser or as a role that can read every table to inspect them.';
    END IF;
END;
$$;

COMMENT ON FUNCTION dbblue_toast_check_indexes(regclass) IS
'List indexes that contain zstd-compressed entries, or whose access method cannot be inspected.';

--
-- Fast check, now also covering nested values and indexes.  Index rows have
-- a NULL columnname and name the index in "reason".
--
CREATE OR REPLACE FUNCTION dbblue_toast_check(relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, columnname text, reason text)
LANGUAGE plpgsql
AS $$
DECLARE
    col     record;
    ix      record;
    found   boolean;
    skipped oid[] := '{}';
    targets oid[] := dbblue_toast_targets(relation);
BEGIN
    FOR col IN SELECT * FROM dbblue_toastable_columns c
                WHERE targets IS NULL OR c.reloid = ANY(targets)
                ORDER BY 1, 2, 5
    LOOP
        IF NOT pg_catalog.has_table_privilege(col.reloid, 'SELECT') THEN
            IF NOT col.reloid = ANY(skipped) THEN
                skipped := skipped || col.reloid;
            END IF;
            CONTINUE;
        END IF;

        IF col.attcompression = 'z' THEN
            schemaname := col.schemaname; tablename := col.tablename;
            columnname := col.columnname; reason := 'column is marked zstd';
            RETURN NEXT;
        END IF;

        /* an unpopulated matview holds no data, and cannot be scanned */
        CONTINUE WHEN col.relkind = 'm' AND NOT
            (SELECT relispopulated FROM pg_catalog.pg_class WHERE oid = col.reloid);

        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM ONLY %s
                             WHERE dbblue_datum_has_zstd(%I))',
            col.reloid::regclass, col.columnname)
          INTO found;

        IF found THEN
            schemaname := col.schemaname; tablename := col.tablename;
            columnname := col.columnname; reason := 'contains zstd values';
            RETURN NEXT;
        END IF;
    END LOOP;

    IF cardinality(skipped) > 0 THEN
        RAISE WARNING 'skipped % table(s) you do not have SELECT privilege on', cardinality(skipped)
          USING HINT = 'Run this as a superuser or as a role that can read every table, otherwise zstd data in them goes unreported.';
    END IF;

    FOR ix IN SELECT * FROM dbblue_toast_check_indexes(relation) LOOP
        schemaname := ix.schemaname; tablename := ix.tablename;
        columnname := NULL; reason := ix.reason;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION dbblue_toast_check(regclass) IS
'List columns that still hold, or are marked for, zstd compression (including inside composite values), and indexes holding zstd entries.';

--
-- Full audit.  Values whose outer compression is not zstd but which contain
-- zstd inside are counted as "zstd (nested)"; indexes holding zstd entries
-- get one row each, with a NULL columnname.
--
CREATE OR REPLACE FUNCTION dbblue_toast_audit(relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, columnname text,
               compression text, nvalues bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    col     record;
    ix      record;
    skipped oid[] := '{}';
    targets oid[] := dbblue_toast_targets(relation);
BEGIN
    FOR col IN SELECT * FROM dbblue_toastable_columns c
                WHERE targets IS NULL OR c.reloid = ANY(targets)
                ORDER BY 1, 2, 5
    LOOP
        IF NOT pg_catalog.has_table_privilege(col.reloid, 'SELECT') THEN
            IF NOT col.reloid = ANY(skipped) THEN
                skipped := skipped || col.reloid;
            END IF;
            CONTINUE;
        END IF;

        /* an unpopulated matview holds no data, and cannot be scanned */
        CONTINUE WHEN col.relkind = 'm' AND NOT
            (SELECT relispopulated FROM pg_catalog.pg_class WHERE oid = col.reloid);

        RETURN QUERY EXECUTE format(
            'SELECT %L::text, %L::text, %L::text,
                    CASE WHEN pg_catalog.pg_column_compression(%I) IS DISTINCT FROM ''zstd''
                              AND dbblue_datum_has_zstd(%I)
                         THEN ''zstd (nested)''
                         ELSE coalesce(pg_catalog.pg_column_compression(%I), ''<uncompressed>'')
                    END::text,
                    count(*)::bigint
               FROM ONLY %s
              WHERE %I IS NOT NULL
              GROUP BY 4',
            col.schemaname, col.tablename, col.columnname,
            col.columnname, col.columnname, col.columnname,
            col.reloid::regclass, col.columnname);
    END LOOP;

    IF cardinality(skipped) > 0 THEN
        RAISE WARNING 'skipped % table(s) you do not have SELECT privilege on', cardinality(skipped)
          USING HINT = 'Run this as a superuser or as a role that can read every table, otherwise they are missing from the audit.';
    END IF;

    FOR ix IN SELECT * FROM dbblue_toast_check_indexes(relation) LOOP
        schemaname := ix.schemaname; tablename := ix.tablename;
        columnname := NULL;
        compression := CASE WHEN ix.zstd_entries IS NULL
                            THEN format('unknown (index %s)', ix.indexname)
                            ELSE format('zstd (index %s)', ix.indexname) END;
        nvalues := ix.zstd_entries;
        RETURN NEXT;
    END LOOP;
END;
$$;

--
-- Relations a materialized view reads from, looking through views, with
-- partitions mapped to their partition root.  Functions called by the query
-- are not tracked, which is why normalize also re-checks after REFRESH.
--
CREATE FUNCTION dbblue_matview_sources(mv regclass)
RETURNS SETOF oid
LANGUAGE sql STABLE
AS $$
    WITH RECURSIVE src(rel) AS (
        SELECT d.refobjid
          FROM pg_catalog.pg_rewrite r
          JOIN pg_catalog.pg_depend d
            ON d.classid = 'pg_catalog.pg_rewrite'::regclass AND d.objid = r.oid
           AND d.refclassid = 'pg_catalog.pg_class'::regclass
         WHERE r.ev_class = mv AND d.refobjid <> mv
        UNION
        SELECT d.refobjid
          FROM src
          JOIN pg_catalog.pg_class c ON c.oid = src.rel AND c.relkind = 'v'
          JOIN pg_catalog.pg_rewrite r ON r.ev_class = c.oid
          JOIN pg_catalog.pg_depend d
            ON d.classid = 'pg_catalog.pg_rewrite'::regclass AND d.objid = r.oid
           AND d.refclassid = 'pg_catalog.pg_class'::regclass
         WHERE d.refobjid <> c.oid
    )
    SELECT DISTINCT CASE WHEN c.relispartition
                         THEN pg_catalog.pg_partition_root(c.oid)::oid
                         ELSE c.oid END
      FROM src
      JOIN pg_catalog.pg_class c ON c.oid = src.rel
     WHERE c.relkind IN ('r', 'm', 'p')
$$;

COMMENT ON FUNCTION dbblue_matview_sources(regclass) IS
'Tables and materialized views a materialized view reads from, through views.';

-- One column of one relation that normalize has to rewrite
CREATE TYPE dbblue_toast_candidate AS (
    tgt            oid,
    columnname     text,
    coltype        text,
    relkind        "char",
    attcompression "char",
    tgtschema      text,
    tgtname        text
);

--
-- Rewrite every zstd value to another compression method.
--
-- Order of work:
--   1. tables: ALTER COLUMN TYPE, or UPDATE where ALTER COLUMN TYPE is
--      refused (dependent views, policies, triggers, generated columns,
--      partition keys, inherited columns, typed tables);
--   2. materialized views, each only once every relation it reads from is
--      clean -- REFRESH copies compressed values verbatim, so refreshing
--      earlier would just copy the zstd data again;
--   3. REINDEX of indexes still holding zstd entries.  This runs last and
--      re-inspects, because ALTER COLUMN TYPE in step 1 already rebuilds
--      the indexes on the columns it rewrites.  A dry run lists indexes as
--      they are now, so it may list some that step 1 would rebuild anyway.
--
-- Each statement runs in its own subtransaction: a relation that fails is
-- reported with method 'failed: <error>' and a WARNING, matviews reading it
-- are skipped, and everything else is still normalized.  All of it still
-- commits together at the end, so every rewritten table stays locked (ACCESS
-- EXCLUSIVE for a rewrite) until the call returns.  On a large database use
-- CALL dbblue_toast_normalize_run() instead, which commits after each table.
--
CREATE OR REPLACE FUNCTION dbblue_toast_normalize(target text DEFAULT 'pglz',
                                                  dry_run boolean DEFAULT true,
                                                  relation regclass DEFAULT NULL)
RETURNS TABLE (schemaname text, tablename text, columnname text,
               method text, command text)
LANGUAGE plpgsql
/*
 * With row security on, an UPDATE on a table whose policies apply to us
 * would silently skip the rows we cannot see and leave them zstd.  Off, it
 * fails instead, and that failure is reported for that table.
 */
SET row_security = off
AS $$
DECLARE
    cands      dbblue_toast_candidate[];
    cand       dbblue_toast_candidate;
    table_tgts oid[];
    pending    oid[];
    still      oid[];
    mv         oid;
    src        oid;
    blocker    oid;
    waiting    boolean;
    progress   boolean;
    stmt       text;
    set_stmts  text;
    cols       text;
    mvschema   text;
    mvname     text;
    populated  boolean;
    has_dep    boolean;
    inherited  boolean;
    generated  boolean;
    src_col    text;
    gen_expr   text;
    tgt_kind   "char";
    upd_stmt   text;
    errmsg     text;
    failed     oid[] := '{}';
    ix         record;
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
    /*
     * Within a table, generated columns go last: rewriting the columns they
     * are computed from recomputes them, which usually leaves nothing to do.
     */
    cands := ARRAY(
        SELECT d.cand
          FROM (SELECT DISTINCT ON (s.tgt, s.columnname)
                       s.tgt, s.columnname,
                       ROW(s.tgt, s.columnname, s.coltype, s.relkind, s.attcompression,
                           n.nspname::text, r.relname::text)::dbblue_toast_candidate AS cand
                  FROM (SELECT c.columnname, c.coltype, c.attcompression, c.relkind,
                               CASE WHEN c.relispartition
                                    THEN pg_catalog.pg_partition_root(c.reloid)::oid
                                    ELSE c.reloid END AS tgt
                          FROM dbblue_toastable_columns c
                         WHERE (c.schemaname, c.tablename, c.columnname) IN
                               (SELECT k.schemaname, k.tablename, k.columnname
                                  FROM dbblue_toast_check(relation) k)) s
                  JOIN pg_catalog.pg_class r ON r.oid = s.tgt
                  JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                 ORDER BY s.tgt, s.columnname) d
          LEFT JOIN pg_catalog.pg_attribute a
                 ON a.attrelid = d.tgt AND a.attname = d.columnname
         ORDER BY d.tgt, coalesce(a.attgenerated, '') <> '', d.columnname);

    table_tgts := ARRAY(SELECT DISTINCT u.tgt FROM unnest(cands) u WHERE u.relkind <> 'm');
    pending    := ARRAY(SELECT DISTINCT u.tgt FROM unnest(cands) u WHERE u.relkind = 'm');

    /* 1. tables */
    FOREACH cand IN ARRAY cands
    LOOP
        CONTINUE WHEN cand.relkind = 'm';

        /*
         * ALTER COLUMN TYPE is refused outright when the column is
         *   - used by a view, matview or rule          (pg_rewrite dependency)
         *   - used in a row-level security policy      (pg_policy dependency)
         *   - used in a trigger WHEN clause or UPDATE OF list
         *                                              (pg_trigger dependency)
         *   - used by a generated column               (pg_attrdef dependency;
         *                                               a plain DEFAULT cannot
         *                                               reference a column)
         *   - part of the partition key                (pg_partitioned_table;
         *                                               an expression key counts
         *                                               for every column, which
         *                                               is merely cautious)
         *   - inherited from a parent (INHERITS)       (attinhcount > 0)
         *   - in a typed table (CREATE TABLE ... OF)   (reloftype <> 0)
         * Those tables fall back to an UPDATE, which has no such restriction
         * and takes a weaker lock -- at the cost of dead tuples to vacuum
         * afterwards, and of firing row triggers, which is why it is not the
         * default and is reported in "method".
         */
        SELECT a.attinhcount > 0,
               a.attgenerated <> '',
               c.relkind,
               a.attinhcount > 0
               OR c.reloftype <> 0
               OR EXISTS (SELECT 1
                            FROM pg_catalog.pg_depend d
                           WHERE d.refclassid = 'pg_class'::regclass
                             AND d.refobjid = cand.tgt
                             AND d.refobjsubid = a.attnum
                             AND (d.classid IN ('pg_rewrite'::regclass,
                                                'pg_policy'::regclass,
                                                'pg_trigger'::regclass)
                                  /* 'i' is a generated column's own expression */
                                  OR (d.classid = 'pg_attrdef'::regclass
                                      AND d.deptype = 'n')))
               OR EXISTS (SELECT 1
                            FROM pg_catalog.pg_partitioned_table pt
                           WHERE pt.partrelid = cand.tgt
                             AND (a.attnum = ANY (pt.partattrs::int2[])
                                  OR pt.partexprs IS NOT NULL))
          INTO inherited, generated, tgt_kind, has_dep
          FROM pg_catalog.pg_attribute a
          JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
         WHERE a.attrelid = cand.tgt
           AND a.attname = cand.columnname;

        schemaname := cand.tgtschema; tablename := cand.tgtname;
        columnname := cand.columnname;

        /*
         * Rewriting an INHERITS parent recurses into its children (both the
         * SET COMPRESSION and the TYPE change), so a child processed after
         * its parent may already be clean.  Likewise a generated column is
         * recomputed when a column it is computed from is rewritten.
         */
        IF NOT dry_run AND (inherited OR generated)
           AND NOT EXISTS (SELECT 1 FROM dbblue_toast_check(cand.tgt::regclass) k
                            WHERE k.columnname = cand.columnname) THEN
            method := CASE WHEN generated
                           THEN 'skipped: already recomputed from its source columns'
                           ELSE 'skipped: already rewritten through its parent' END;
            command := NULL;
            RETURN NEXT;
            CONTINUE;
        END IF;

        /*
         * Only override an explicit zstd marking; a column left on the
         * default is already covered by default_toast_compression, which we
         * checked above is no longer zstd.
         */
        set_stmts := '';
        IF cand.attcompression = 'z' THEN
            set_stmts := format('ALTER TABLE %I.%I ALTER COLUMN %I SET COMPRESSION %s; ',
                                cand.tgtschema, cand.tgtname, cand.columnname, target);
        END IF;

        /*
         * ONLY: every inheritance child is a candidate of its own.  A
         * partitioned table has no rows of its own, so it must not use ONLY.
         */
        upd_stmt := format('UPDATE %s%I.%I SET %I = dbblue_force_detoast(%I);',
                           CASE WHEN tgt_kind = 'p' THEN '' ELSE 'ONLY ' END,
                           cand.tgtschema, cand.tgtname,
                           cand.columnname, cand.columnname);

        IF generated THEN
            /*
             * A generated column cannot be assigned, nor rewritten with
             * USING, and SET col = DEFAULT does not recompute it.  It is
             * recomputed, and compressed afresh, when a column it is computed
             * from is updated -- so update one of those to itself.  An
             * expression reading no column at all is recomputed by
             * re-setting it, which rewrites the table.
             */
            SELECT src.attname, pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)
              INTO src_col, gen_expr
              FROM pg_catalog.pg_attribute g
              JOIN pg_catalog.pg_attrdef ad
                ON ad.adrelid = g.attrelid AND ad.adnum = g.attnum
              LEFT JOIN LATERAL (
                     SELECT a2.attname
                       FROM pg_catalog.pg_depend d
                       JOIN pg_catalog.pg_attribute a2
                         ON a2.attrelid = d.refobjid AND a2.attnum = d.refobjsubid
                      WHERE d.classid = 'pg_attrdef'::regclass
                        AND d.objid = ad.oid
                        AND d.refclassid = 'pg_class'::regclass
                        AND d.refobjid = g.attrelid
                        AND d.deptype = 'n'
                      ORDER BY a2.attnum
                      LIMIT 1) src ON true
             WHERE g.attrelid = cand.tgt AND g.attname = cand.columnname;

            IF src_col IS NOT NULL THEN
                method := 'update';
                upd_stmt := format('UPDATE %s%I.%I SET %I = %I;',
                                   CASE WHEN tgt_kind = 'p' THEN '' ELSE 'ONLY ' END,
                                   cand.tgtschema, cand.tgtname, src_col, src_col);
            ELSE
                method := 'rewrite';
                upd_stmt := format('ALTER TABLE %I.%I ALTER COLUMN %I SET EXPRESSION AS (%s);',
                                   cand.tgtschema, cand.tgtname, cand.columnname, gen_expr);
            END IF;
            stmt := set_stmts || upd_stmt;
        ELSIF has_dep THEN
            method := 'update';
            stmt := set_stmts || upd_stmt;
        ELSE
            method := 'rewrite';
            stmt := set_stmts || format(
                'ALTER TABLE %I.%I ALTER COLUMN %I TYPE %s USING dbblue_force_detoast(%I);',
                cand.tgtschema, cand.tgtname, cand.columnname,
                cand.coltype, cand.columnname);
        END IF;

        IF NOT dry_run THEN
            /*
             * One failing table must not roll back the others, so each runs
             * in its own subtransaction.  A refused rewrite is retried as an
             * UPDATE, covering dependency kinds not detected above.
             */
            BEGIN
                EXECUTE stmt;
            EXCEPTION WHEN OTHERS THEN
                errmsg := SQLERRM;
                IF method = 'rewrite' THEN
                    BEGIN
                        stmt := set_stmts || upd_stmt;
                        EXECUTE stmt;
                        method := 'update (rewrite refused: ' || errmsg || ')';
                    EXCEPTION WHEN OTHERS THEN
                        errmsg := SQLERRM;
                        method := 'failed: ' || errmsg;
                    END;
                ELSE
                    method := 'failed: ' || errmsg;
                END IF;

                IF method LIKE 'failed:%' THEN
                    failed := failed || cand.tgt;
                    RAISE WARNING 'could not normalize %.%.%: %',
                                  cand.tgtschema, cand.tgtname, cand.columnname, errmsg
                      USING DETAIL = 'Statement: ' || stmt,
                            HINT = 'The other relations were still normalized.  Fix the cause and run dbblue_toast_normalize again for this relation.';
                END IF;
            END;
        END IF;

        command := stmt;
        RETURN NEXT;
    END LOOP;

    /* 2. materialized views, once their sources are clean */
    LOOP
        EXIT WHEN cardinality(pending) = 0;
        progress := false;
        still := '{}';

        FOREACH mv IN ARRAY pending
        LOOP
            waiting := false;
            blocker := NULL;

            FOR src IN SELECT s FROM dbblue_matview_sources(mv::regclass) s
            LOOP
                IF src = ANY(failed) THEN
                    blocker := src;         -- could not be normalized above
                    EXIT;
                ELSIF src = ANY(pending) AND src <> mv THEN
                    waiting := true;        -- another matview goes first
                    EXIT;
                ELSIF src = ANY(table_tgts) THEN
                    CONTINUE;               -- normalized in step 1
                ELSIF relation IS NOT NULL
                      AND EXISTS (SELECT 1 FROM dbblue_toast_check(src::regclass) k
                                   WHERE k.columnname IS NOT NULL
                                     AND k.reason <> 'column is marked zstd') THEN
                    blocker := src;         -- outside this run, still zstd
                    EXIT;
                END IF;
                /* full run: a source outside the candidates is clean */
            END LOOP;

            IF waiting THEN
                still := still || mv;
                CONTINUE;
            END IF;

            progress := true;

            SELECT string_agg(format('ALTER MATERIALIZED VIEW %I.%I ALTER COLUMN %I SET COMPRESSION %s; ',
                                     u.tgtschema, u.tgtname, u.columnname, target),
                              '' ORDER BY u.columnname)
                     FILTER (WHERE u.attcompression = 'z'),
                   string_agg(u.columnname, ', ' ORDER BY u.columnname),
                   min(u.tgtschema), min(u.tgtname)
              INTO set_stmts, cols, mvschema, mvname
              FROM unnest(cands) u
             WHERE u.tgt = mv;

            schemaname := mvschema; tablename := mvname; columnname := cols;

            IF blocker IS NOT NULL THEN
                method := CASE WHEN blocker = ANY(failed)
                               THEN format('skipped: %s could not be normalized',
                                           blocker::regclass)
                               ELSE format('skipped: %s still holds zstd values; normalize it first',
                                           blocker::regclass) END;
                failed := failed || mv;     -- so views reading this one wait too
                command := NULL;
                RETURN NEXT;
                CONTINUE;
            END IF;

            SELECT relispopulated INTO populated
              FROM pg_catalog.pg_class WHERE oid = mv;

            /* an unpopulated matview holds no data; don't populate it */
            stmt := coalesce(set_stmts, '');
            IF populated THEN
                stmt := stmt || format('REFRESH MATERIALIZED VIEW %I.%I;', mvschema, mvname);
            END IF;
            method := 'refresh';

            IF NOT dry_run AND stmt <> '' THEN
                BEGIN
                    EXECUTE stmt;
                EXCEPTION WHEN OTHERS THEN
                    errmsg := SQLERRM;
                    method := 'failed: ' || errmsg;
                    failed := failed || mv;
                    RAISE WARNING 'could not normalize materialized view %.%: %',
                                  mvschema, mvname, errmsg
                      USING DETAIL = 'Statement: ' || stmt;
                END;

                /* catches sources pg_depend can't see, e.g. inside functions */
                IF method = 'refresh' AND EXISTS (SELECT 1 FROM dbblue_toast_check(mv::regclass) k
                            WHERE k.columnname IS NOT NULL
                              AND k.reason <> 'column is marked zstd') THEN
                    method := 'refresh (incomplete)';
                    RAISE WARNING 'materialized view %.% still holds zstd values after REFRESH',
                                  mvschema, mvname
                      USING HINT = 'Its query reads zstd data from a relation not listed in its dependencies, such as through a function. Normalize that relation, then refresh the view again.';
                END IF;
            END IF;

            command := stmt;
            RETURN NEXT;
        END LOOP;

        IF NOT progress THEN
            /* cannot happen for matviews, but never loop forever */
            FOREACH mv IN ARRAY still
            LOOP
                schemaname := NULL; tablename := mv::regclass::text;
                columnname := NULL; command := NULL;
                method := 'skipped: depends on another materialized view that could not be normalized';
                RETURN NEXT;
            END LOOP;
            EXIT;
        END IF;

        pending := still;
    END LOOP;

    /* 3. indexes still holding zstd entries */
    FOR ix IN SELECT * FROM dbblue_toast_check_indexes(relation)
    LOOP
        stmt := format('REINDEX INDEX %I.%I;', ix.schemaname, ix.indexname);
        method := CASE WHEN ix.zstd_entries IS NULL
                       THEN 'reindex (cannot inspect)' ELSE 'reindex' END;

        IF NOT dry_run THEN
            BEGIN
                EXECUTE stmt;
            EXCEPTION WHEN OTHERS THEN
                errmsg := SQLERRM;
                method := 'failed: ' || errmsg;
                RAISE WARNING 'could not reindex %.%: %',
                              ix.schemaname, ix.indexname, errmsg;
            END;
        END IF;

        schemaname := ix.schemaname; tablename := ix.tablename;
        columnname := NULL; command := stmt;
        RETURN NEXT;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION dbblue_toast_normalize(text, boolean, regclass) IS
'Rewrite zstd-compressed values (including inside composite values, materialized views and indexes) to pglz or lz4, optionally for one relation. Dry run unless dry_run := false.';

--
-- Normalize the whole database, committing after each table, so that each
-- table's lock is released as soon as it is done and work already finished
-- survives an interruption.  Materialized views and indexes are handled in
-- one final pass, since their order depends on each other.  Progress is
-- reported as NOTICEs; the remaining zstd data, if any, as a WARNING.
--
-- Must be CALLed outside an explicit transaction block.
--
CREATE PROCEDURE dbblue_toast_normalize_run(target text DEFAULT 'pglz')
LANGUAGE plpgsql
AS $$
DECLARE
    rel       regclass;
    r         record;
    remaining bigint;
BEGIN
    /* tables, one transaction each; partitions are driven from their root */
    FOR rel IN
        SELECT DISTINCT (CASE WHEN c.relispartition
                              THEN pg_catalog.pg_partition_root(c.reloid)
                              ELSE c.reloid END)::regclass
          FROM dbblue_toast_check() k
          JOIN dbblue_toastable_columns c USING (schemaname, tablename, columnname)
         WHERE c.relkind <> 'm'
    LOOP
        FOR r IN SELECT * FROM dbblue_toast_normalize(target, false, rel) LOOP
            RAISE NOTICE '%: %', concat_ws('.', r.schemaname, r.tablename, r.columnname), r.method;
        END LOOP;
        COMMIT;
    END LOOP;

    /* then materialized views and indexes, in dependency order */
    FOR r IN SELECT * FROM dbblue_toast_normalize(target, false) LOOP
        RAISE NOTICE '%: %', concat_ws('.', r.schemaname, r.tablename, r.columnname), r.method;
    END LOOP;
    COMMIT;

    SELECT count(*) INTO remaining FROM dbblue_toast_check();
    IF remaining > 0 THEN
        RAISE WARNING '% column(s) or index(es) still hold zstd data', remaining
          USING HINT = 'See the "failed" and "skipped" notices above, and SELECT * FROM dbblue_toast_check().';
    END IF;
END;
$$;

COMMENT ON PROCEDURE dbblue_toast_normalize_run(text) IS
'Normalize every table to pglz or lz4, committing after each one so locks are released as it goes.';
