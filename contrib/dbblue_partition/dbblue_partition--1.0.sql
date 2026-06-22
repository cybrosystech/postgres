-- dbblue_partition--1.0.sql
-- Native table partitioning for Odoo/dbblue workloads.
-- Replaces the Odoo Python 'partition_manager' module.

\echo Use "CREATE EXTENSION dbblue_partition" to load this file. \quit

-- ── Catalog tables ────────────────────────────────────────────────────────

-- One row per partitioned table
CREATE TABLE dbblue_partition_catalog (
    id              SERIAL         PRIMARY KEY,
    table_name      TEXT           NOT NULL,
    partition_field TEXT           NOT NULL,
    strategy        TEXT           NOT NULL DEFAULT 'monthly',
    backup_table    TEXT,
    partitioned_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    last_maintained_at TIMESTAMPTZ,
    CONSTRAINT dbblue_catalog_table_uniq   UNIQUE (table_name),
    CONSTRAINT dbblue_catalog_strategy_chk CHECK (strategy IN ('monthly','yearly'))
);

-- One row per child partition
CREATE TABLE dbblue_partition_registry (
    id              SERIAL         PRIMARY KEY,
    catalog_id      INT            NOT NULL
                                   REFERENCES dbblue_partition_catalog(id)
                                   ON DELETE CASCADE,
    partition_name  TEXT           NOT NULL,
    range_start     TIMESTAMPTZ    NOT NULL,
    range_end       TIMESTAMPTZ    NOT NULL,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT dbblue_registry_part_uniq UNIQUE (partition_name)
);

-- ── Main partitioning function ────────────────────────────────────────────
--
-- dbblue_partition_table(table_name, partition_field [, strategy])
--
-- Converts an ordinary table into a range-partitioned table.
-- Steps performed:
--   1. Captures outgoing FKs, incoming FKs, and dependent views
--   2. Creates partitioned parent (LIKE source EXCLUDING CONSTRAINTS,
--      composite PK (id, partition_field))
--   3. Creates child partitions for each existing data period
--   4. Copies all data from source to partitioned table
--   5. Rename swap: original → <table>_backup, partitioned → original
--      5b. Recreates dependent views pointing at the new partitioned table
--   6. Creates a fresh sequence OWNED BY new_table.id (prevents cascade-
--      drop of the default when the backup table is later dropped)
--   7. Re-points incoming FKs from backup to new partitioned table
--   8. Adds outgoing FKs to the partitioned parent (propagates to children)
--   9. Pre-creates future partitions (count = dbblue_partition.advance_count)
--  10. Registers everything in dbblue_partition_catalog / _registry
--
-- The backup table is KEPT after this call; use dbblue_drop_backup() or
-- dbblue_auto_partition() to drop it and recreate indexes in one shot.
--
-- Requires: dbblue_partition.enabled = on
--
CREATE FUNCTION dbblue_partition_table(
    p_table         TEXT,
    p_field         TEXT,
    p_strategy      TEXT DEFAULT NULL   -- NULL → use dbblue_partition.default_strategy
)
RETURNS TEXT
AS '$libdir/dbblue_partition', 'dbblue_partition_table'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

-- ── Safe backup drop ──────────────────────────────────────────────────────
--
-- dbblue_drop_backup(table_name)
--
-- Drops the _backup table created by dbblue_partition_table().
-- Because the sequence is now OWNED BY table_name.id (not the backup),
-- this DROP does NOT cascade to the sequence or its DEFAULT values.
--
CREATE FUNCTION dbblue_drop_backup(
    p_table TEXT
)
RETURNS VOID
AS '$libdir/dbblue_partition', 'dbblue_drop_backup'
LANGUAGE C VOLATILE STRICT;

-- ── Future partition maintenance ──────────────────────────────────────────
--
-- dbblue_ensure_partitions(table_name [, advance_count])
--
-- Creates future child partitions up to advance_count periods ahead of now().
-- Returns the number of new partitions created (0 if all already exist).
-- advance_count defaults to dbblue_partition.advance_count GUC.
--
CREATE FUNCTION dbblue_ensure_partitions(
    p_table         TEXT,
    p_advance       INT  DEFAULT NULL   -- NULL → use dbblue_partition.advance_count
)
RETURNS INT
AS '$libdir/dbblue_partition', 'dbblue_ensure_partitions'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

-- ── GUC-driven auto partition ────────────────────────────────────────────
--
-- dbblue_auto_partition()
--
-- Zero-argument entry point: reads the table/column/range from GUCs and
-- runs the complete partitioning flow in one call — equivalent to the
-- Odoo partition_manager module's "Generate Partitions" button.
--
-- Usage:
--   SET dbblue_partition.enabled = on;
--   SET dbblue_partition.table   = 'sale_order';
--   SET dbblue_partition.column  = 'date_order';
--   SET dbblue_partition.range   = 'monthly';   -- or 'yearly'
--   SELECT dbblue_auto_partition();
--
-- GUC parameters (all prefixed with dbblue_partition.):
--   table   TEXT     ''         Target table name (required)
--   column  TEXT     ''         Partition column  (required, must be NOT NULL date/timestamp)
--   range   TEXT     'monthly'  Granularity: 'monthly' or 'yearly'
--
-- This function does everything dbblue_partition_table() does, PLUS:
--   - Drops the backup table automatically
--   - Recreates non-unique, non-PK indexes on the partitioned parent
--     (PostgreSQL propagates them to all child partitions)
--
-- Requires: dbblue_partition.enabled = on
--
CREATE FUNCTION dbblue_auto_partition()
RETURNS TEXT
AS '$libdir/dbblue_partition', 'dbblue_auto_partition'
LANGUAGE C VOLATILE STRICT;

-- ── Partition status view ─────────────────────────────────────────────────
--
-- dbblue_partition_info(table_name)
--
-- Returns one row per child partition with row counts and size.
--
CREATE FUNCTION dbblue_partition_info(
    p_table TEXT
)
RETURNS TABLE (
    partition_name  TEXT,
    range_start     TEXT,
    range_end       TEXT,
    live_rows       BIGINT,
    dead_rows       BIGINT,
    size            TEXT
)
AS '$libdir/dbblue_partition', 'dbblue_partition_info'
LANGUAGE C STABLE STRICT;
