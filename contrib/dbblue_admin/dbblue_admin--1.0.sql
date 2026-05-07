/* contrib/dbblue_admin/dbblue_admin--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dbblue_admin" to load this file. \quit

--
-- dbblue_auto_partition_status
--
-- Surfaces the auto_partition_* reloptions for every relation that has
-- them set to anything other than off. Useful as a quick "what's
-- partitioned in this cluster" check during operations.
--
-- Returns one row per configured relation. Phase 1 only stores the
-- reloptions; the partition manager that acts on them is Phase 2.5+.
--
CREATE FUNCTION dbblue_auto_partition_status(
    OUT schemaname        name,
    OUT relname           name,
    OUT strategy          text,
    OUT partition_column  text,
    OUT partition_interval text,
    OUT retention         integer)
RETURNS SETOF record
LANGUAGE sql
STABLE
AS $$
    SELECT
        n.nspname,
        c.relname,
        opts.config->>'auto_partition_strategy',
        opts.config->>'auto_partition_column',
        opts.config->>'auto_partition_interval',
        NULLIF(opts.config->>'auto_partition_retention', '')::int
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN LATERAL (
        SELECT jsonb_object_agg(
                   split_part(o, '=', 1),
                   substring(o from position('=' in o) + 1))
               AS config
        FROM unnest(c.reloptions) o
        WHERE o LIKE 'auto_partition%'
    ) opts ON TRUE
    WHERE c.relkind = 'r'
      AND opts.config IS NOT NULL
      AND opts.config ? 'auto_partition_strategy'
      AND opts.config->>'auto_partition_strategy' <> 'off'
    ORDER BY n.nspname, c.relname;
$$;

COMMENT ON FUNCTION dbblue_auto_partition_status() IS
    'Lists relations with DBblue auto_partition_* reloptions set';

-- Convenience view for "show me everything"
CREATE VIEW dbblue_auto_partition AS
    SELECT * FROM dbblue_auto_partition_status();

COMMENT ON VIEW dbblue_auto_partition IS
    'View wrapping dbblue_auto_partition_status() for ad-hoc queries';
