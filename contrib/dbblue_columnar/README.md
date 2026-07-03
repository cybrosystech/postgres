# dbblue_columnar — DBblue Columnar Engine

An AlloyDB-inspired, in-memory columnar accelerator for DBblue, built as a
preloaded module. **Standalone by design** — it depends only on core
PostgreSQL, never on the IVM engine. (The two may later integrate at the
matview-storage seam, but nothing here requires IVM.)

## Status: Milestone 1 skeleton

A *loadable, buildable, do-nothing* engine to grow from. It currently:

- registers the `dbblue_columnar.*` GUCs;
- registers a `CustomScan` provider that offers **no path yet**, so the planner
  falls through to normal Seq/Index/Bitmap scans;
- registers a background refresh worker that **idles**;
- provides `dbblue_columnar_add(rel regclass, columns text[])`, which records
  registrations in `dbblue_columnar_relations` (no column store is built yet).

Nothing here changes query results — it is inert on purpose.

## Build

Built in-tree as a contrib module:

```sh
make -C contrib/dbblue_columnar
make -C contrib/dbblue_columnar install
```

## Enable

```
# postgresql.conf
shared_preload_libraries = 'dbblue_columnar'
dbblue_columnar.enabled = on          # restart required (PGC_POSTMASTER)
dbblue_columnar.memory_mb = 512
```

```sql
CREATE EXTENSION dbblue_columnar;
SELECT dbblue_columnar_add('account_move_line', ARRAY['company_id','date','balance']);
SELECT * FROM dbblue_columnar_status;
```

## GUCs

| GUC | Default | Context | Purpose |
|---|---|---|---|
| `dbblue_columnar.enabled` | `off` | postmaster (restart) | master switch |
| `dbblue_columnar.enable_columnar_scan` | `on` | user | let the planner read columnar (no restart) |
| `dbblue_columnar.auto_columnarize` | `off` | sighup | auto column selection |
| `dbblue_columnar.memory_mb` | `128` | postmaster (restart) | column-store memory budget |

## Design

See `ALLOYDB_COLUMNAR_ENGINE_ARCHITECTURE.md` (research) in the DBblue repo root
for the reference architecture this engine is modeled on. Correctness in later
milestones rests on the **visibility map + page LSN**: a column block is served
only when its heap pages are all-visible *and* their page LSN is unchanged since
the block was built; otherwise the scan falls back to the heap.
