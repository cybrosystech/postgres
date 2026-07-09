# dbblue_columnar — DBblue Columnar Engine

An AlloyDB-inspired, in-memory columnar accelerator for DBblue, built as a
preloaded module. **Standalone by design** — it depends only on core
PostgreSQL, never on the IVM engine. (The two may later integrate at the
matview-storage seam, but nothing here requires IVM.)

## Status: Milestone 2, step 1 — the column store exists

The in-memory column store is real: `dbblue_columnar_populate(rel)` builds
columnar blocks (32 heap pages each) in a shared DSA for the registered
columns, with per-page **build LSN stamps**, per-chunk **zone maps**
(min/max/null-count), null bitmaps, and plain (encoding-ready) storage for
fixed-width and varlena columns. Only page ranges that are entirely
**all-visible with valid LSNs** are built; everything else stays heap-only.
Budget-accounted (`dbblue_columnar.memory_mb`), error-safe (a canceled
populate frees everything it built), and introspectable:

- `dbblue_columnar_add(rel, columns[])` — register columns (validates; rejects
  system columns)
- `dbblue_columnar_populate(rel)` — build/rebuild the store (rejects
  unlogged/temp relations and non-{1,2,4,8,16}-width fixed types, loudly)
- `dbblue_columnar_blocks(rel)` — per-(block, chunk) introspection incl. zone maps
- `dbblue_columnar_drop(rel)` — free a relation's store (works by OID after
  DROP TABLE); registrations are kept
- `dbblue_columnar_memory()` — budget, logical bytes, real DSA bytes

The planner still offers **no columnar path** (that is the next step), so
query results are unchanged. The refresh worker still idles.

## Aggregate pushdown (Milestone 2 step 4)

Scalar `count(*)` / `count(col)` / `min(col)` / `max(col)` (no `GROUP BY`,
`HAVING`, or `WHERE`) over a populated columnar relation are answered by a
`Custom Scan (DBBlueColumnarAgg)` upper node **from block metadata** — a valid
block contributes `nrows` (count(\*)), `nrows − null_count` (count(col)), or its
zone-map min/max — with no value reads. Invalid / unbuilt / type-changed ranges
are read from the heap with the query snapshot.

Notes:
- **`SUM`/`AVG` are intentionally not pushed** into this node. Their transition
  functions require a real `AggState`; and they already run fast on the normal
  `Agg → DBBlueColumnarScan` plan (zone-skip + no heap deform). The custom node
  is only for aggregates a normal Agg can't answer without scanning.
- **`MIN`/`MAX` tie representation:** for types with equal-but-distinguishable
  values (numeric `4.0`/`4.00`, float `-0.0`/`0.0`), which representation is
  returned is unspecified by SQL and varies across PostgreSQL plans anyway. The
  value is always equal-by-ordering to the true extremum.
- Inheritance/partition parents are left to the normal Append (no per-partition
  columnar acceleration yet).

## Background auto-refresh (Milestone 4)

A background worker keeps the store fresh with no manual `populate`. Enable it
per database:

```
# postgresql.conf (autorefresh_database is read once at startup)
dbblue_columnar.autorefresh_database = 'mydb'
dbblue_columnar.naptime = 60             # seconds between passes
dbblue_columnar.refresh_threshold = 20   # percent of coverage drift to trigger a rebuild
```

Each pass builds registered-but-unbuilt relations and rebuilds stale ones
(staleness = the gap between currently all-visible pages and the pages the
version covers). Concurrent `populate` of the same relation is serialized with
`ShareUpdateExclusiveLock` (never blocks readers).

Known v1 limitations:
- **One database per worker** (fixed at startup); a cluster-wide launcher is
  future work.
- **Full rebuild**, not incremental block reuse.
- Staleness is visibility-map based, so blocks that were modified *and then
  re-vacuumed* (all-visible again but with a newer page LSN) are still served
  correctly from the heap but are not eagerly re-columnarized until the relation
  next grows/changes or you run `dbblue_columnar_populate()` manually.

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
