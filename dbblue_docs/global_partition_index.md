# Global Partition Index

A **global partition index** is one physical btree that covers every leaf
partition of a RANGE-partitioned table. It exists in DBblue so that a UNIQUE
index or constraint that does **not** contain the partition key can be enforced
across all partitions. Upstream PostgreSQL rejects such an index.

This document explains why the feature exists, how it works internally, what it
does not support, and what testing found. Read it before changing any of the
files listed in [File map](#7-file-map).

---

## 1. Why DBblue needs it

DBblue is a PostgreSQL fork tuned for Odoo. Large Odoo tables (`sale_order`,
`account_move_line`, …) are converted to RANGE partitions on `create_date` by
the `dbblue_partition` extension (`CALL dbblue_partition_model('sale.order')`).

After the conversion, Odoo still believes the table is an ordinary table. During
a module upgrade it re-issues its uniqueness DDL:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS sale_order_ref_uniq ON sale_order (client_order_ref);
ALTER TABLE sale_order ADD CONSTRAINT sale_order_name_uniq UNIQUE (name, company_id);
```

Upstream PostgreSQL fails both statements:

```
ERROR: unique constraint on partitioned table must include all partitioning columns
```

A per-partition unique index is not a fix either. It lets the same `name`
exist once in every year's partition.

With global indexes, DBblue **silently turns these statements into a GLOBAL
index**. There is one btree for the whole table, and uniqueness is checked
across all partitions, so the upgrade succeeds and the business rule holds.

```
NOTICE:  creating unique constraint "sale_order_name_uniq" on partitioned table "sale_order" as a GLOBAL index
DETAIL:  The index does not include the partition key, so uniqueness is enforced across all partitions by a global index.
```

Only a few PostgreSQL forks have global indexes; upstream does not.

---

## 2. Using it

| What | How |
|---|---|
| Automatic (the Odoo path) | `CREATE UNIQUE INDEX … ON parent (cols)` or `ALTER TABLE parent ADD CONSTRAINT … UNIQUE (cols)` where `cols` does not contain the partition column |
| Explicit | `CREATE [UNIQUE] INDEX GLOBAL name ON parent (cols)` (`GLOBAL` comes right after `INDEX`) |
| Turn auto-conversion off | `SET dbblue_auto_global_index = off` gives the upstream error back |
| Is an index global? | `SELECT indexrelid::regclass FROM pg_index WHERE indglobal;` or `pg_get_indexdef()` prints `CREATE UNIQUE INDEX GLOBAL …` (`\d` does **not** mark it) |

Supported index shapes include plain columns, expressions (`lower(name)`),
partial indexes (`WHERE active`), `INCLUDE (…)`, and `NULLS NOT DISTINCT`. A
UNIQUE constraint created this way lives on the parent (`pg_constraint`) and is
backed by the global index.

Not converted (the upstream behaviour is kept): unique indexes that already
contain the partition key, PRIMARY KEY, EXCLUDE, DEFERRABLE constraints, and
non-btree indexes. Non-unique indexes stay ordinary partitioned indexes unless
you write `GLOBAL` explicitly.

---

## 3. The big picture

```
                           sale_order  (partitioned parent, relkind 'p', no heap)
                               │
     ┌─────────────────────────┼──────────────────────────┐
     │                         │                          │
 sale_order_p2023        sale_order_p2024           sale_order_p2025      ← leaf heaps
 (heap, TIDs (0,1)…)     (heap, TIDs (0,1)…)        (heap, TIDs (0,1)…)
     ▲                         ▲                          ▲
     │ route by create_date    │                          │
     └──────────────┬──────────┴──────────────────────────┘
                    │
     sale_order_name_uniq   (ONE btree, relkind 'i', indrelid = parent, indglobal = true)
     entry = ( name, company_id, create_date ) → TID
               └── user keys ──┘ └ routing key ┘
```

A normal partitioned index is only a catalog entry, with one real btree per
partition. A global index is the opposite: **one real btree, attached to the
storage-less parent**, whose entries point into many different heaps.

That creates the central problem of the design. **A TID such as `(7,3)` exists
in every partition**, so it doesn't say which heap a row is in. The answer is
the **trailing partition key column**: every entry also stores the row's
`create_date`, and the code routes that value through the parent's partition
bounds to find the owning partition. Almost every piece of the feature is built
on this routing step.

---

## 4. Physical layout

### 4.1 Catalog

* `pg_index.indglobal` (new bool column, `src/include/catalog/pg_index.h`)
  marks the index.
* The index is a real `RELKIND_INDEX` whose `indrelid` is the parent.
  It has no `pg_inherits` children.
* `DefineIndex()` **appends the partition key column(s) as trailing key
  columns**. `indkey` for `UNIQUE (name, company_id)` is `name company_id create_date`
  and `indnkeyatts = 3`. The column is appended even if the user's columns
  already contain it, so the trailing columns are always the routing columns.
* `IndexGlobalNumUserKeys()` (`catalog/index.c`) returns the number of
  **user** key columns (2 in the example). Everything user-facing uses it:
  uniqueness checks, `pg_get_indexdef`/`pg_get_constraintdef` (`ruleutils.c`),
  error messages (`genam.c`), and `CREATE TABLE … LIKE` (`parse_utilcmd.c`).

### 4.2 Why the partition key is a *key* column, not INCLUDE

nbtree requires every entry to be unique by *(key columns, heap TID)*. TIDs
repeat across partitions, so two rows with the same user key at the same TID in
different partitions would produce byte-identical entries. Partitions don't
overlap, so adding the partition key as a key column makes them distinct again.
It also puts the routing value in every entry.

### 4.3 Btree specifics

* **btree only.** Routing, the uniqueness check and vacuum read btree pages.
* **Deduplication is off** (`allequalimage = false` in the metapage).
  A posting list would merge TIDs from different heaps into one tuple, which
  the per-entry routing and purge code can't handle.
* **No simple or bottom-up deletion** (`nbtinsert.c`). Those passes check TID
  liveness against the one heap passed to the insert, which is wrong for a
  global index.
* The metapage is written by hand into `MAIN_FORKNUM`
  (`write_global_index_metapage`). `ambuildempty()` would write the unlogged
  init fork, and `index_build()` would scan the parent, which has no storage.

---

## 5. Life of the index: code paths

### 5.1 Creation

```
DefineIndex()                                   commands/indexcmds.c
 ├─ UniqueIndexNeedsGlobal()  → auto-convert (GUC dbblue_auto_global_index)
 ├─ guards: RANGE on one plain column, btree, not PK/EXCLUDE/DEFERRABLE/CONCURRENTLY
 ├─ append partition key as trailing key column
 └─ index_create_percona(flags |= INDEX_CREATE_SKIP_BUILD | INDEX_CREATE_GLOBAL)   catalog/index.c
     ├─ pg_index.indglobal = true
     ├─ write_global_index_metapage()          empty btree, dedup off
     ├─ build_global_index()                   unless INDEX_CREATE_GLOBAL_NOFILL
     │    for each leaf partition (ShareLock):
     │       gpi_check_partition_supported()   no sub-partitioned / foreign partitions
     │       gpi_fill_one_partition()
     │          BuildGlobalIndexInfo()         column numbers mapped to this partition
     │          table_index_build_scan(…, gpi_build_callback)
     │             per row: index_insert(UNIQUE_CHECK_NO) + ExecCheckGlobalIndexUnique()
     └─ invalidate every partition's relcache  (HOT-blocking columns changed, §5.4)
```

`index_create_percona()` is the real body of upstream `index_create()`. The
pg_tde patch set renamed it to add an `old_rlocator` argument, and
`index_create()` is a thin wrapper that errors when pg_tde is loaded.

`INDEX_CREATE_GLOBAL_NOFILL` is used when `ALTER TABLE` recreates the index and
is about to rewrite the partitions anyway (e.g. `ALTER COLUMN id TYPE bigint`).
In that case the rewrite refills the index (§5.6).

### 5.2 Routing an entry to its partition

`execIndexing.c`:

* `ExecGlobalIndexRouteToIndex(partkey, partdesc, gidx, itup)` finds the
  partition key column inside the index tuple, reads its value, and searches
  the partition bounds (`partition_range_datum_bsearch`, falling back to the
  DEFAULT partition). It returns the partition's **position** in `partdesc`,
  or -1. HASH and LIST branches exist but are unreachable today.
* `ExecGlobalIndexRoutePartition(parent, gidx, itup, include_detached)` is a
  wrapper that fetches `partdesc` and returns the partition **OID**.

The index scan uses the first form with its own cached descriptor, so its
positions stay consistent with its per-partition arrays. Purge, vacuum and the
unique check use the second form.

### 5.3 Write path (INSERT / UPDATE / COPY / MERGE / logical replication)

```
ExecOpenIndices()  → ExecOpenGlobalIndexes()   once per partition per statement:
                     open parent's global indexes + BuildGlobalIndexInfo (cached on ResultRelInfo)
ExecInsertIndexTuples()
  for each global index:
     skip if HOT update (EIIT_ONLY_SUMMARIZING), skip if partial predicate is false
     index_insert(UNIQUE_CHECK_NO)
     if UNIQUE: ExecCheckGlobalIndexUnique()
```

Callers that previously tested only `ri_NumIndices > 0` now also test
`ri_NumGlobalIndices` (`nodeModifyTable.c`, `copyfrom.c`, `execReplication.c`,
`execPartition.c`). A partition with no local index can still have global ones.

**`ExecCheckGlobalIndexUnique()`** replaces btree's own uniqueness check, which
only knows one heap:

1. If the key contains NULL and the index is `NULLS DISTINCT`, there can be no
   conflict, so return.
2. Scan the global index for entries with the same user key. Route each one,
   skip the new row's own entry, and collect *(partition, TID)* candidates. No
   other partition is opened while the index page is pinned, which avoids a
   deadlock with VACUUM FULL that the deadlock detector can't see.
3. For each candidate, open its partition and fetch the TID with a **dirty
   snapshot**. Then convert the row's layout if needed, check the predicate,
   recompute the key, and compare it with `global_index_keys_equal()`. Entries
   can be stale, so the row is always rechecked.
4. If the matching row belongs to a transaction still in progress, wait for it
   and restart. If it's a live duplicate, raise `unique_violation`. During a
   build or attach, the error text is "could not create unique index".

Because the entry is inserted *before* the check, two sessions inserting the
same key into different partitions at the same moment can see each other and
wait on each other. One of them gets `deadlock_detected` instead of
`unique_violation`.

### 5.4 HOT updates

An UPDATE runs on the leaf partition, whose own index list doesn't include the
parent's global index. Without help, the leaf would treat a change to a
globally indexed column as HOT-eligible and skip index maintenance. The global
index would then silently go stale.
`AddParentGlobalIndexHotBlockingAttrs()` (`utils/cache/relcache.c`) adds the
parent's global-index columns (keys, INCLUDE, expression and predicate columns,
mapped by name) to each leaf's HOT-blocking bitmap. Creating a global index
invalidates every partition's relcache, so already-connected backends pick
this up.

### 5.5 Read path

Planner:

* `allpaths.c`: for a partitioned rel with a global index, `create_index_paths()`
  is also run on the **parent** rel. This happens only for plain `SELECT`
  without row marks. UPDATE/DELETE targets and `SELECT … FOR UPDATE` don't use
  global indexes (EvalPlanQual needs per-child row marks).
* `plancat.c`: global indexes get real AM properties, but only the serial
  **Index Scan** is allowed. Index Only Scan, bitmap scan, parallel scan and
  mark/restore are disabled.
* `indxpath.c` / `planner.c`: skip storage-less partitioned indexes when costing,
  and keep the parent-level global paths when the rel's target changes.

Executor (`nodeIndexscan.c`):

* `ExecInitIndexScan` builds a `GlobalIndexPartState`. It opens every
  partition, including ones being detached concurrently, and records which
  ones are `visible` to the query.
* `IndexNext`, for each entry: route it to a partition (skip if invisible),
  then `GlobalIndexPreparePartition()` lazily creates that partition's fetch
  descriptor, block count and tuple-conversion map. It skips entries pointing
  past the end of the heap, fetches the row by TID, converts it to the
  parent's layout (copying `tts_tableOid`/`tts_tid`), and **always rechecks
  the qual**, because entries can be stale.

### 5.6 Maintenance and partition lifecycle

| Event | What keeps the global index correct | Where |
|---|---|---|
| `VACUUM` of a leaf | dead TIDs of that leaf are removed. The btree bulk-delete callback gets the whole index tuple (`GIVacCallback`), so it deletes only entries that are dead **and** route to that leaf | `vacuumlazy.c` (`gi_should_delete`), `nbtree.c`, `genam.h` |
| `CREATE TABLE … PARTITION OF` | nothing to do (empty). Rejected if the new partition is partitioned or foreign | `tablecmds.c` (`DefineRelation`) |
| `ATTACH PARTITION` | backfill the partition's rows (`IndexGlobalAttachPartition`) | `tablecmds.c`, `index.c` |
| `DETACH PARTITION` | purge entries routing to it before `RemoveInheritance` (`IndexGlobalDetachPartition` → `gi_purge_routed`) | `tablecmds.c`, `index.c` |
| `DETACH … CONCURRENTLY` | purge in `DetachPartitionFinalize`. Until then the partition is detach-pending, still routed to, but invisible to queries | `tablecmds.c` |
| `DROP` / `TRUNCATE` a partition or the parent | purge (TRUNCATE: purge, then nothing to add) | `tablecmds.c` |
| Heap rewrite: `VACUUM FULL`, `CLUSTER`, `REPACK`, `ALTER TABLE` rewrite | `IndexGlobalResyncPartition` = purge old TIDs, then backfill new ones | `repack.c`, `tablecmds.c` |
| `REINDEX` of the global index | new relfilenode, metapage, `build_global_index()` | `index.c` (`reindex_index`) |

The purge is a full bulk-delete pass over the whole global index, whatever the
partition's size.

---

## 6. Limitations (by design, today)

| Area | Limitation |
|---|---|
| Partitioning | Only `PARTITION BY RANGE` on **one plain column**. LIST, HASH, multi-column and expression keys are rejected |
| Hierarchy | One level only. No sub-partitioned or foreign-table partitions, neither before nor after the global index exists |
| Index type | btree only. Deduplication disabled |
| Constraints | No PRIMARY KEY, EXCLUDE or DEFERRABLE global constraints |
| `INSERT … ON CONFLICT` | A global index is never an arbiter. `ON CONFLICT (col)` fails with "no unique or exclusion constraint matching", and **`ON CONFLICT DO NOTHING` without a target raises `unique_violation`** instead of skipping |
| Foreign keys | A global unique index cannot be the referenced key of an FK |
| Concurrency of DDL | No `CREATE INDEX CONCURRENTLY` and no `REINDEX CONCURRENTLY` |
| Scans | Serial Index Scan only: no Index Only, bitmap or parallel scans. **UPDATE/DELETE and `SELECT … FOR UPDATE` never use a global index** and fall back to per-partition scans (a seq scan when no local index exists) |
| Display | `\d` doesn't mark the index GLOBAL. `pg_class.reltuples` of the index stays `-1` after ANALYZE |
| Cost | Build is ~10× slower than a local index (row-by-row inserts); ATTACH/DETACH scale with the whole index (see §9) |
| `dbblue_partition` | The extension itself never creates global indexes. Existing unique indexes become per-partition (template) indexes; global ones come from later Odoo DDL |

---

## 7. File map

| File | Role |
|---|---|
| `src/backend/parser/gram.y`, `ecpg.header` | `CREATE [UNIQUE] INDEX GLOBAL`. Adds one shift/reduce conflict (`%expect 1`): an index named `global` must now be quoted |
| `src/include/catalog/pg_index.h`, `catversion.h`, `share/postgres.bki` | `indglobal` column |
| `src/include/nodes/parsenodes.h`, `pathnodes.h`, `execnodes.h` | `IndexStmt.global`, `IndexOptInfo.indglobal`, `ResultRelInfo.ri_*GlobalIndex*` |
| `src/backend/commands/indexcmds.c` | auto-conversion, guards, trailing key, flags |
| `src/backend/catalog/index.c` | metapage, build/backfill, attach/detach/resync, REINDEX, `IndexGlobalNumUserKeys`, `BuildGlobalIndexInfo` |
| `src/backend/executor/execIndexing.c` | routing, insert maintenance, `ExecCheckGlobalIndexUnique` |
| `src/backend/executor/nodeIndexscan.c` | routed index scan |
| `src/backend/executor/nodeModifyTable.c`, `execPartition.c`, `execReplication.c`, `commands/copyfrom.c` | open and maintain global indexes for partitions |
| `src/backend/optimizer/path/allpaths.c`, `indxpath.c`, `plan/planner.c`, `util/plancat.c` | planning global index paths on the parent |
| `src/backend/access/nbtree/nbtree.c`, `nbtinsert.c`, `access/index/genam.c`, `include/access/genam.h` | routing-aware bulk delete, no deletion passes, error key text |
| `src/backend/access/heap/vacuumlazy.c` | VACUUM of a leaf cleans the parent's global indexes |
| `src/backend/commands/tablecmds.c`, `repack.c` | partition lifecycle and rewrites |
| `src/backend/utils/cache/relcache.c` | HOT-blocking columns from the parent's global indexes |
| `src/backend/utils/adt/ruleutils.c`, `parser/parse_utilcmd.c` | `pg_get_indexdef`/`constraintdef`, `LIKE … INCLUDING INDEXES` |
| `src/backend/catalog/system_views.sql` | `pg_stat_*_indexes` include global indexes |
| `src/backend/utils/misc/guc_parameters.dat`, `guc_tables.c` | `dbblue_auto_global_index` |

---

## 8. Known bugs (found 2026-09-30 / 2026-10-01)

Severity is from an Odoo production point of view.

| # | Severity | Bug | Repro | Cause, and fix direction |
|---|---|---|---|---|
| 1 | **Critical** | Rolling back DETACH, DROP partition or TRUNCATE (of a partition or the parent) **deletes index entries permanently** | `BEGIN; TRUNCATE part; ROLLBACK;` → entries 3000 → 1906, lookups miss rows, uniqueness lost. `TRUNCATE parent` + rollback → 0 entries | `IndexGlobalDetachPartition` bulk-deletes immediately, which is not transactional. Defer the purge to pre-commit (pending list, like ON COMMIT actions). Readers already skip rows of gone partitions |
| 2 | **Critical** | A failed or rolled-back ATTACH leaves its backfilled entries behind. Re-attaching duplicates them; if it's never re-attached they become permanent orphans | attach fails or rolls back, then attach again → the row is returned twice, `bt_index_check`: "item order invariant violated" | `IndexGlobalAttachPartition` inserts without first purging. Purge, then fill (like `IndexGlobalResyncPartition`); also handle aborted attaches |
| 3 | **Critical** | `ALTER TABLE … ALTER COLUMN … TYPE` on a column of a global index **when no rewrite is needed** (e.g. `varchar(50)` → `varchar(80)`) crashes the backend on cassert builds (all sessions reset). On release builds it reads out of bounds | `ALTER TABLE d ALTER COLUMN code TYPE varchar(80);` → `TRAP: Assert("old_natts == numberOfAttributes")`, `indexcmds.c:296` | `CheckIndexCompatible()` compares the user columns with `indnkeyatts`, which includes the trailing key. Compare only `IndexGlobalNumUserKeys()` columns, or return false for global indexes. Odoo changes varchar sizes during upgrades |
| 4 | **High** | **pg_dump loses global UNIQUE constraints.** It writes `ADD CONSTRAINT … UNIQUE (name, company_id, create_date)`, so the restore creates a per-partition constraint and cross-partition uniqueness is silently gone | dump + restore → `sale_order_name_uniq` is `UNIQUE (name, company_id, create_date)`, `indglobal = f` | `pg_dump.c` (`dumpConstraint`, ~line 18891) lists all `indnkeyattrs` columns. Make pg_dump global-aware (user keys only), or use `pg_get_constraintdef()`. `CREATE UNIQUE INDEX GLOBAL` statements are dumped correctly |
| 5 | Medium | `INSERT … ON CONFLICT DO NOTHING` (no target) raises `unique_violation` on a global-index conflict | see §6 | global indexes aren't considered as arbiters |
| 6 | Medium | Concurrent inserts of the same key can fail with `deadlock_detected` instead of `unique_violation`, and each one costs `deadlock_timeout` | 8 sessions × 400 inserts on 300 keys: 6 deadlocks, 6.2 s vs 0.2 s for a plain table. No duplicates | insert-then-check design (§5.3). Apps that retry only on unique_violation won't retry |
| 7 | Medium | `dbblue_partition_undo` drops unique indexes that Odoo added after partitioning | undo after the Odoo-upgrade flow → only the conversion-time indexes come back | the undo procedure restores the captured index set only |
| 8 | Low | UPDATE/DELETE/FOR UPDATE by a globally indexed column seq-scan every partition | `EXPLAIN UPDATE d … WHERE code = 'x'` | planner restriction (§5.5) |
| 9 | Low | Confusing messages: CONCURRENTLY prints the auto-convert NOTICE and then fails; a hash GLOBAL index says "does not support multicolumn indexes"; a failed ATTACH says "could not create unique index" | — | cosmetic |
| 10 | Low | `\d` doesn't show GLOBAL; `reltuples` stays -1; stale comments (`INCLUDE'd partition key` in `nodeIndexscan.c`/`allpaths.c`, "starts empty" in `indexcmds.c:1371`) | — | cosmetic |

Workaround for 1 and 2 until they are fixed: avoid DETACH, DROP, TRUNCATE and
ATTACH of partitions inside transactions that might roll back. Run
`REINDEX INDEX <global index>` if one did. REINDEX was verified to repair the
index.

---

## 9. Test results

148 checks were run on a scratch cluster built from `staging_y` (cassert build,
with pg_tde, pg_partman and dbblue_partition loaded). **129 passed.** Every
failure is listed in §8. The groups:

| Group | Pass / total | Covers |
|---|---|---|
| G1 limits | 27 / 28 | auto-conversion rules, rejected shapes, sub-partitions, foreign tables, dedup off, backfill |
| G2 Odoo flow | 37 / 38 | `dbblue_partition` conversion, Odoo's `ADD CONSTRAINT` + `COMMENT ON CONSTRAINT`, `constraint_definition()` round trip, `IF NOT EXISTS` idempotence, expression/partial/NULLS NOT DISTINCT/INCLUDE, drop and re-add, pg_partman `run_maintenance`, undo |
| G3 DML | 30 / 34 | HOT safety, cross-partition UPDATE, DEFAULT partition, NULLs, ON CONFLICT, MERGE, ordered and backward scans, joins |
| G4 maintenance | 14 / 16 | VACUUM, VACUUM FULL, CLUSTER, rewrites, type changes, REINDEX variants, LIKE |
| G5 lifecycle | 8 / 17 | create/attach/detach/detach concurrently/drop/truncate and their rollbacks |
| G6 concurrency | 5 / 6 | waits, commit/rollback interleavings, 8-session stress |
| G7 durability | 3 / 4 | pg_dump/restore, crash (immediate stop) under load + recovery |
| G8 performance | 5 / 5 | measurements below (informational) |

Performance (1M rows, 24 monthly partitions, global `UNIQUE (id)` vs local
`UNIQUE (id, d)`):

| Operation | Global | Local | |
|---|---|---|---|
| Lookup `WHERE id = ?` (pgbench, 4 clients, prepared) | 12,106 tps | 2,787 tps | 4.3× faster |
| Lookup, simple protocol | 4,664 tps | 2,053 tps | 2.3× faster |
| Buffers per lookup | 4 | 50 | one btree vs 24 |
| Index build | 4.53 s | 0.43 s | 10× slower |
| Index size | 33 MB | 22 MB | |
| 100k single-row inserts | 6.17 s | 3.99 s | 1.5× slower |
| 200k-row INSERT … SELECT | 1.65 s | 0.77 s | 2.1× slower |
| DETACH a 41k-row partition | 188 ms | 1 ms | full index purge |
| ATTACH it back | 463 ms | 10 ms | row-by-row backfill |

### How to verify an index yourself

```sql
CREATE EXTENSION pageinspect; CREATE EXTENSION amcheck;
SELECT bt_index_check('sale_order_name_uniq');           -- structure
SELECT allequalimage FROM bt_metap('sale_order_name_uniq'); -- false = dedup off
-- entries vs rows: count leaf items with bt_page_items (skip the high key);
-- run VACUUM (INDEX_CLEANUP ON) first, because plain VACUUM may bypass index cleanup.
```

Build and test notes for this tree: it has no header dependency tracking, so
do a clean backend rebuild after editing any `.h`. `make check` fails because
initdb preloads contrib libraries, so test with `installcheck` against a
scratch cluster.
