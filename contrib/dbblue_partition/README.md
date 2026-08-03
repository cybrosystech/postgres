# dbblue_partition

Odoo-aware, in-place conversion of an existing table to native range
partitioning.  One call:

```sql
SET dbblue_partition.enabled = on;
CALL dbblue_partition_model('sale.order', 'create_date', '1 month');
```

and `sale_order` is a range-partitioned table with everything preserved —
data, indexes, constraints, foreign keys in both directions, triggers,
views, privileges, RLS policies, comments, sequences, publication
membership, replica identity — and future partitions maintained
automatically by [pg_partman](../pg_partman/), which is vendored unmodified
at `contrib/pg_partman/`.

pg_partman deliberately refuses in-place conversion (`create_parent()`
requires an already-partitioned parent).  This extension fills exactly that
gap: the validate/lock/capture/swap/reattach choreography lives here, the
partition lifecycle (gapless premake, DEFAULT partition, gap fill, batched
data move, retention, background maintenance) is delegated to pg_partman.

## Interface

| Call | Purpose |
|---|---|
| `CALL dbblue_partition_model(model, control, interval, schema, premake, batch_interval, single_transaction, analyze, odoo_compat)` | Convert a table. Only the first argument is required; defaults are `create_date`, `1 month`, `public`, 4 premade partitions. Finishes by configuring the Odoo compatibility view for the database owner (opt out with `p_odoo_compat => false`). |
| `SELECT * FROM dbblue_partition_status([model])` | Progress/inspection: state, rows moved, partition count, DEFAULT-partition rows, backup state. Never throws for dropped objects. |
| `SELECT dbblue_partition_drop_backup(model)` | Drop `<table>_old` once the conversion is complete and the backup is empty (`p_force => true` overrides). |
| `CALL dbblue_partition_undo(model)` | Restore the original plain table while the backup still exists. Single transaction, atomic. |
| `SELECT dbblue_partition_odoo_compat(role)` | Make an unmodified Odoo accept partitioned model tables: per-role catalog-shadowing view (see trade-off 6). |
| `SELECT dbblue_partition_odoo_compat_remove(role)` | Undo the above. Pass the role, or the role keeps a search_path pointing at the dropped schema. |
| `SELECT * FROM dbblue_partition_odoo_compat_check()` | Is the Odoo compatibility layer actually in effect? Run it after every restore (see below). |

`model` accepts an Odoo model name (`'sale.order'`) or a table name
(`'sale_order'`, `'Part Case'` — case and spaces preserved).

All *mutating* entry points refuse to run until `dbblue_partition.enabled = on`
— off by default, per DBblue convention.  `dbblue_partition_status()` is
read-only and stays available for monitoring.  The GUC deliberately avoids
reserved SQL keywords, so `SET`/`SHOW` always work.

## How a conversion runs

1. **Validate everything first** — table exists and is an ordinary table,
   caller owns it, control column exists with a date/timestamp type and no
   NULLs, a primary key exists, no inheritance/rules/exclusion
   constraints/materialized-view dependents, names fit, not already
   managed.  Any failure here leaves the database untouched.
2. **`LOCK TABLE ... IN ACCESS EXCLUSIVE MODE`**, then capture every
   dependent object as SQL text.
3. Rename the table to `<table>_old` (its indexes to `*_old`, so the new
   table keeps the original index names), create the partitioned
   replacement under the original name with `LIKE ... INCLUDING ALL EXCLUDING
   INDEXES` (indexes and the primary key are rebuilt explicitly in step 3),
   rebuild the PK as `(pk columns, control)`, recreate indexes and FKs,
   and call `pg_partman.create_parent()` starting from the oldest existing
   row — the partition set is gapless from day one and a DEFAULT partition
   catches everything else, so the table is writable immediately, even
   when it was empty.
4. Commit the structural swap, then move the data from `<table>_old` in
   batches with `pg_partman.partition_data_time()`, committing between
   batches.  Interrupted?  `CALL dbblue_partition_model(...)` again to
   resume; `dbblue_partition_status()` shows progress.
5. Verify counts — an empty backup is enforced; a lower live row count only
   warns, since the application may legitimately delete rows mid-migration.
   Then `partition_gap_fill()`, `VALIDATE` the re-pointed FKs, ANALYZE,
   mark complete.  The backup stays until you drop it explicitly.

Conversion state lives in `dbblue_partition_catalog`, registered with
`pg_extension_config_dump()` so `pg_dump` carries it.

## Documented trade-offs — read before partitioning

These are deliberate and inherent to PostgreSQL partitioning; do not "fix"
them here.

1. **The PK widens to `(id, create_date)`.**  PostgreSQL requires every
   unique index on a partitioned table to include the partition key.
   Odoo's single-column FKs (`REFERENCES sale_order(id)`) keep working
   thanks to a DBblue core patch (`transformFkeyCheckAttrs()` accepts a
   unique index whose *only* surplus columns are partition key columns),
   but the FK is then backed by a non-unique key: **if duplicate ids ever
   appear across partitions, referential actions misbehave** (`ON DELETE
   CASCADE` can delete children whose parent still exists).  Odoo draws id
   from a sequence, so this does not happen in normal operation.  Run this
   after migrations, `setval()` changes, or restores:

   ```sql
   SELECT id, count(*) FROM <table> GROUP BY id HAVING count(*) > 1;
   ```

   The real fix is global indexes — see the `feature/global_partition_index`
   branch.

2. **UNIQUE constraints without the partition column become per-partition.**
   They are applied to every partition through pg_partman's template table
   (`dbblue_tpl_<table>` — do not drop it; the name is prefixed rather
   than suffixed because Odoo has real `<table>_template` models).  Two
   rows with the same `name`
   in *different* months are no longer rejected.  A WARNING is emitted per
   affected constraint at conversion time.

3. **The batched data move is not atomic** (the structural swap is).  Rows
   wait in `<table>_old` and stream into the partition set batch by batch;
   a crash mid-way leaves a resumable, consistent state, protected by row
   count verification.  Use `p_single_transaction => true` for
   all-or-nothing semantics at the price of holding ACCESS EXCLUSIVE for
   the whole copy (fine for small tables; it also works inside an explicit
   transaction block).
   During a batched migration, re-pointed incoming FKs are created `NOT
   VALID` (new writes are checked immediately; existing rows are validated
   at the end), and historical rows are briefly invisible to the
   application until their batch lands.  That also means a write into a
   *referencing* table (e.g. a new order line for a not-yet-moved order)
   fails its FK check until that order's batch lands.  Run it in a
   maintenance window.

4. **Logical replication**: moved rows are re-published as inserts.  If
   the table is in a publication you get a WARNING; consider
   `publish_via_partition_root = true` and deduplication on subscribers.

5. **Odoo's `DROP NOT NULL` on the partition key is ignored** (WARNING
   instead of error) by a DBblue core patch in `ATExecDropNotNull()`,
   because Odoo issues it unconditionally for non-required fields and the
   column must stay NOT NULL as part of the PK.  The catalog is never
   falsified — the statement is declined, not faked.

6. **Odoo ORM recognition** — solved database-side, no Odoo change needed.
   Odoo's schema introspection reads relkind from the *unqualified* name
   `pg_class` and only accepts `'r'`, so a partitioned model table looks
   missing and module updates try to `CREATE TABLE` over it.  relkind can
   never be faked in the real catalog (the planner, pg_dump and pg_partman
   branch on it), but it can be translated for Odoo's eyes only:

   `dbblue_partition_model()` configures this automatically at the end of
   every conversion, targeting the database owner (an Odoo database is
   always owned by the Odoo db_user) — restart Odoo afterwards so its
   connections pick it up.  It can also be run by hand for another role:

   ```sql
   SELECT dbblue_partition_odoo_compat('odoo_role');   -- then restart Odoo
   ```

   Either way it creates a `dbblue_compat.pg_class` view reporting `'r'` for tables
   range-partitioned on a single `create_date` column, and sets that
   role's search_path to `"$user", public, dbblue_compat, pg_catalog` in
   the current database.  Because `pg_catalog` is listed explicitly, the
   view shadows the catalog for that role only — the planner, pg_dump,
   psql `\d` and pg_partman (whose functions pin their own search_path)
   all keep seeing the truth.  `dbblue_partition_odoo_compat_remove(role)`
   undoes it — pass the role, or it keeps a search_path pointing at the
   dropped schema.

   **Re-apply it after any restore.**  The view is an ordinary object that
   `pg_dump` carries, but the per-role search_path lives in
   `pg_db_role_setting`, which plain `pg_dump` does not carry — and it is
   keyed by database *name*, so it does not apply to a restore under a
   different name either.  A restored Odoo database keeps its partitions
   but loses the illusion, and module updates start failing again with
   *relation already exists*.  Diagnose and fix with:

   ```sql
   SELECT * FROM dbblue_partition_odoo_compat_check();  -- verdict says what to run
   ```

   One consequence to be aware of: listing `pg_catalog` explicitly is what
   allows the view to shadow the catalog, and it necessarily places
   `pg_catalog` *after* `public` for that role.  Anything in `public`
   sharing a name with a builtin function, operator or type would then win
   name resolution there; both compat functions report such objects when
   they find any.

## Known small print

- Not carried to the new table (everything else is): comments on the PK
  index and on per-partition template unique indexes, per-column `ALTER
  TABLE ... SET STATISTICS` targets, `CLUSTER` markings, and ACLs granted
  directly on an identity column's sequence (the new table gets a fresh
  identity sequence, value-synchronized).  Privileges are replayed with
  the converting role as grantor, collapsing multi-grantor ACL chains.
- `dbblue_partition_undo()` requires the backup and the live table to
  still have the same column set; if module updates added or dropped
  columns after conversion, undo stops with the exact difference and asks
  for manual reconciliation.
- Concurrent deletes during a batched migration are legal; the final
  verification uses moved-row accounting and only warns about the lower
  live count.

## Ongoing maintenance

Partitions must keep being created ahead of time.  The supported way on a
DBblue cluster is DBblue's own worker (`src/dbblue_partition_bgw.c`) —
everything an operator configures stays in the `dbblue_partition.*`
namespace; the vendored pg_partman engine underneath is an implementation
detail:

```
shared_preload_libraries = 'dbblue_partition_bgw'
dbblue_partition.maintenance_dbname   = 'odoo'   # comma-separated for several DBs
dbblue_partition.maintenance_interval = 3600     # seconds, reloadable with SIGHUP
dbblue_partition.maintenance_role     = ''       # '' = cluster superuser
```

One worker per listed database calls pg_partman's `run_maintenance()` on
the interval, resolving pg_partman's schema at run time.  With no
database configured the library loads but starts nothing (off by
default, per DBblue convention).  Preloading it also declares
`dbblue_partition.enabled` as a real GUC, so `SHOW` works before any
`SET`.  (pg_partman's own `pg_partman_bgw` still builds but is not meant
to be preloaded on DBblue.)

Alternatively schedule pg_partman's `run_maintenance()` via cron, in whichever
schema pg_partman was installed into (`SELECT dbblue_partition_partman_schema();`).

Retention (dropping/detaching old partitions) is configured directly in
pg_partman's `part_config` table (`retention`, `retention_schema`,
`retention_keep_table`) — see pg_partman's documentation.

## Known upstream issue (pg_partman 5.4.3)

`partition_data_time()` has a `format()` bug in its `p_lock_wait > 0`
branch (`%6$L` with only five arguments), so this extension always calls it
with `p_lock_wait := 0` — harmless here because nothing else can write to
the renamed backup table.  Additionally, its batch windows compare
timestamps as text, which is only correct under ISO DateStyle; the
conversion procedure pins `datestyle = 'ISO'` for its session while data is
moving.  Both are worth reporting/patching when the vendored copy is next
updated.

## Files

- `dbblue_partition--1.0.sql` — the original base script, plus the catalog
  table, the helper functions and `dbblue_partition_drop_backup()`, which
  have not changed since.  Like every released version script it is
  immutable: fixes go into a new update script, never here.
- `dbblue_partition--1.0--1.1.sql`, `--1.1--1.2.sql`, `--1.2--1.3.sql` —
  update scripts.  **The newest one is authoritative** for anything it
  redefines: `--1.2--1.3.sql` currently holds `dbblue_partition_convert()`,
  `_model()`, `_status()`, `_undo()`, the Odoo compatibility functions and
  `_odoo_compat_check()`.  A fresh `CREATE EXTENSION` runs the whole chain,
  so both a new install and an upgrade end on identical definitions; after
  changing a version script, confirm that with

  ```sql
  -- in two databases, then diff the output
  SELECT p.proname, md5(pg_get_functiondef(p.oid)) FROM pg_proc p
  JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
  JOIN pg_extension e ON e.oid = d.refobjid AND e.extname = 'dbblue_partition'
  ORDER BY 1;
  ```

  SQL + PL/pgSQL only: every identifier goes through `format()` `%I`/`%L`
  and every function pins `search_path`, except the two procedures — a
  procedure carrying a `SET` clause cannot COMMIT — whose bodies
  schema-qualify everything instead, exactly like pg_partman's own.
- `src/dbblue_partition_bgw.c` — the maintenance worker (the only C in the
  extension; see "Ongoing maintenance").
- `sql/`, `expected/` — regression tests (`make check` in this directory);
  every scenario corresponds to a defect of the retired first-generation
  `dbblue_partition` C extension (data loss under concurrency, dropped
  UNIQUE/CHECK/generated/index/trigger properties, partition coverage
  gaps, unquoted identifiers, decomposed multi-column FKs, unusable GUC
  names, catalog not in pg_dump, ...).
