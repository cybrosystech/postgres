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
| Is an index global? | `\d table` shows `GLOBAL` at the end of its line; also `SELECT indexrelid::regclass FROM pg_index WHERE indglobal;` or `pg_get_indexdef()` (`CREATE UNIQUE INDEX GLOBAL …`) |

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

### 5.3a INSERT … ON CONFLICT

A global unique index can be an ON CONFLICT arbiter (`ON CONFLICT (cols)`,
`ON CONSTRAINT name`, or none for `DO NOTHING`).

* **Planner** (`infer_arbiter_indexes`, `plancat.c`): global indexes are
  candidates; their columns are matched without the trailing partition key.
* **Partition setup** (`ExecInitPartitionInfo`, `execPartition.c`): a global
  arbiter has no copy in the partition, so it stays in the partition's arbiter
  list as is.
* **Pre-check** (`ExecInsert`, `nodeModifyTable.c`): after the partition's own
  arbiters (`ExecCheckIndexConstraints`), `ExecCheckGlobalIndexConstraints()`
  looks for the key across all partitions, waiting for in-progress inserters,
  and returns the conflicting row's partition and TID.
* **Conflict in another partition**: `ExecGetGlobalConflictPartition()` routes
  the conflicting row to get that partition's `ResultRelInfo`, with its ON
  CONFLICT projections, RETURNING and RLS, and converts the proposed row
  (`EXCLUDED`) to its layout. The unchanged `ExecOnConflictUpdate` /
  `ExecOnConflictSelect` / DO NOTHING code then locks, checks `WHERE`, updates
  or returns that row. A DO UPDATE that changes the partition key is refused
  ("invalid ON UPDATE specification"), as upstream.
* **Speculative insertion**: if no conflict was found, the row is inserted
  speculatively. In `ExecInsertIndexTuples` a global arbiter then only flags
  `specConflict`, without waiting (`global_index_find_conflict(…, wait = false)`).
  `ExecInsert` backs the row out and redoes the pre-check, which waits for the
  other row. This is the same protocol upstream uses for btree arbiters, which
  is why concurrent upserts neither fail nor deadlock.

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
| `ATTACH PARTITION` (also MERGE/SPLIT PARTITIONS) | rewrite: keep all entries except ones routing to the partition (leftovers), add its rows, check uniqueness (`IndexGlobalAttachPartition`) | `tablecmds.c`, `index.c` |
| `DETACH PARTITION` | rewrite without the entries routing to it, before `RemoveInheritance` (`IndexGlobalDetachPartition`) | `tablecmds.c`, `index.c` |
| `DETACH … CONCURRENTLY` | the same rewrite in `DetachPartitionFinalize`. Until then the partition is detach-pending, still routed to, but invisible to queries | `tablecmds.c` |
| `DROP` / `TRUNCATE` a partition or the parent | rewrite without the partition's entries; TRUNCATE re-adds its (now empty) heap | `tablecmds.c` |
| Heap rewrite: `VACUUM FULL`, `CLUSTER`, `REPACK` | rewrite, replacing the partition's entries with ones for the new heap (`IndexGlobalResyncPartition`) | `repack.c`, `tablecmds.c` |
| `ALTER TABLE` that rewrites partitions | one rewrite per parent after all partitions are rewritten, with a uniqueness check, since column types may have changed (`IndexGlobalResyncPartitions`) | `tablecmds.c` |
| `REINDEX` of the global index | new relfilenode, metapage, `build_global_index()` | `index.c` (`reindex_index`) |

**All of these are transactional.** None of them edits the global index in
place. `gpi_rewrite_global_index()` (`catalog/index.c`):

1. Walks the old btree's leaf pages (`_bt_global_collect`, `nbtsort.c`) and
   feeds every entry that should survive into a tuplesort. Entries that route
   to no partition (leftovers) are dropped on the way.
2. Adds the current rows of the partitions being (re)added, via
   `table_index_build_scan`.
3. Gives the index new storage (`RelationSetNewRelfilenumber`, as REINDEX does)
   and writes it as a sorted build (`_bt_global_load` → `_bt_load`).
4. For ATTACH and ALTER TABLE, checks the added rows' uniqueness.

The old file is unlinked only at commit. On ROLLBACK, an error, or ROLLBACK
TO SAVEPOINT, the new file is discarded and the index is exactly as before.
(An earlier version bulk-deleted or backfilled in place, which a rollback
could not undo; see §8, bugs 1 and 2.)

Costs: each rewrite reads and rewrites the whole global index (~0.5–0.9 s per
operation on a 1.3M-row table) and holds `AccessExclusiveLock` on it, like
REINDEX. While it runs, queries through the global index and writes to *any*
partition wait. A sorted rewrite also leaves the index compact (40 MB → 26 MB
in the test).

---

## 6. Limitations (by design, today)

| Area | Limitation |
|---|---|
| Partitioning | Only `PARTITION BY RANGE` on **one plain column**. LIST, HASH, multi-column and expression keys are rejected |
| Hierarchy | One level only. No sub-partitioned or foreign-table partitions, neither before nor after the global index exists. No UNLOGGED partitions either: crash recovery empties them but not the global index |
| Index type | btree only. Deduplication disabled |
| Constraints | No PRIMARY KEY, EXCLUDE or DEFERRABLE global constraints |
| `INSERT … ON CONFLICT` | Supported since 2026-10-01 for `DO NOTHING`, `DO UPDATE` and `DO SELECT`, with `ON CONFLICT (cols)`, `ON CONSTRAINT` or no target. The conflicting row may be in another partition; it is updated where it is. As upstream, a `DO UPDATE` that would move the row to another partition is refused. A conflict target (`ON CONFLICT (cols)`) is inferred only when inserting through the partitioned parent; an `INSERT` straight into a partition with `DO NOTHING` and no target still skips a duplicate in a sibling partition (fixed 2026-10-06) |
| Foreign keys | A global unique index cannot be the referenced key of an FK |
| Concurrency of DDL | No `CREATE INDEX CONCURRENTLY` and no `REINDEX CONCURRENTLY` (`REINDEX TABLE/SCHEMA/DATABASE CONCURRENTLY` skip global indexes with a WARNING; use `REINDEX INDEX`). No `CLUSTER` on a global index |
| Scans | Serial Index Scan only: no Index Only, bitmap or parallel scans. **UPDATE/DELETE and `SELECT … FOR UPDATE` never use a global index** and fall back to per-partition scans (a seq scan when no local index exists) |
| Display | `\d` marks global indexes `GLOBAL`; statistics are kept by ANALYZE of the parent and by VACUUM of its partitions (fixed 2026-10-05) |
| Cost | Build is ~10× slower than a local index (row-by-row inserts); ATTACH/DETACH/DROP/TRUNCATE/VACUUM FULL of a partition rewrite the whole global index and block writes to all partitions meanwhile (see §5.6, §9) |
| Concurrency risks (not bugs; former item 18, none reproduced) | **Lock order:** DDL on one partition (ATTACH/DETACH/DROP/TRUNCATE/CLUSTER/VACUUM FULL) locks the partition, then the shared global index; DML locks the global index, then may open a sibling partition for the uniqueness check, and a global scan opens every partition after the index. A deadlock is possible in a narrow window; PostgreSQL detects it (`40P01`) and Odoo retries. Relation locks last until commit. Run partition maintenance with a short `lock_timeout` and retry. **ON CONFLICT:** two upserts of the same key could in theory keep backing out (insert-then-check, no "older XID wins" rule as upstream's `CEOUC_LIVELOCK_PREVENTING_WAIT`); 16 sessions on one key never triggered it. **SERIALIZABLE:** a rewrite moves predicate locks to the partitioned parent, which writers into partitions don't check, so a read/write conflict across a rewrite could be missed. Not applicable to Odoo, which runs at REPEATABLE READ (`odoo/sql_db.py`), nor to dbblue_partition (READ COMMITTED) |
| `dbblue_partition` | Since 1.1, `dbblue_partition_undo` recreates indexes and unique constraints added after the conversion as ordinary ones on the restored table. The extension itself never creates global indexes. Existing unique indexes become per-partition (template) indexes; global ones come from later Odoo DDL |

---

## 7. File map

| File | Role |
|---|---|
| `src/backend/parser/gram.y`, `ecpg.header` | `CREATE [UNIQUE] INDEX GLOBAL`. Adds one shift/reduce conflict (`%expect 1`): an index named `global` must now be quoted |
| `src/include/catalog/pg_index.h`, `catversion.h`, `share/postgres.bki` | `indglobal` column |
| `src/include/nodes/parsenodes.h`, `pathnodes.h`, `execnodes.h` | `IndexStmt.global`, `IndexOptInfo.indglobal`, `ResultRelInfo.ri_*GlobalIndex*` |
| `src/backend/commands/indexcmds.c` | auto-conversion, guards, trailing key, flags |
| `src/backend/catalog/index.c` | metapage, build/backfill, attach/detach/resync, REINDEX, `IndexGlobalNumUserKeys`, `BuildGlobalIndexInfo` |
| `src/backend/executor/execIndexing.c` | routing, insert maintenance, `ExecCheckGlobalIndexUnique`, ON CONFLICT pre-check `ExecCheckGlobalIndexConstraints` |
| `src/backend/executor/nodeIndexscan.c` | routed index scan |
| `src/backend/executor/nodeModifyTable.c`, `execPartition.c`, `execReplication.c`, `commands/copyfrom.c` | open and maintain global indexes for partitions; ON CONFLICT through a global arbiter (`ExecGetGlobalConflictPartition`) |
| `src/backend/optimizer/path/allpaths.c`, `indxpath.c`, `plan/planner.c`, `util/plancat.c` | planning global index paths on the parent |
| `src/backend/access/nbtree/nbtree.c`, `nbtinsert.c`, `access/index/genam.c`, `include/access/genam.h` | routing-aware bulk delete, no deletion passes, error key text |
| `src/backend/access/nbtree/nbtsort.c`, `include/access/nbtree.h` | `_bt_global_collect` / `_bt_global_load`: copy surviving entries and write them as a sorted build into new storage |
| `src/test/regress/sql/global_partition_index.sql` | regression test: rollbacks of DETACH/DROP/TRUNCATE/ATTACH/rewrites, ATTACH retry, ALTER TYPE |
| `src/backend/access/heap/vacuumlazy.c` | VACUUM of a leaf cleans the parent's global indexes |
| `src/backend/commands/tablecmds.c`, `repack.c` | partition lifecycle and rewrites |
| `src/backend/utils/cache/relcache.c` | HOT-blocking columns from the parent's global indexes |
| `src/bin/pg_dump/pg_dump.c`, `pg_dump.h` | constraints on a global index are dumped with their own columns only (`indnconkeyattrs`) |
| `src/bin/psql/describe.c` | `\d` shows `GLOBAL` |
| `src/backend/commands/analyze.c` | ANALYZE of a partitioned table updates its global indexes' statistics |
| `src/backend/utils/adt/ruleutils.c`, `parser/parse_utilcmd.c` | `pg_get_indexdef`/`constraintdef`, `LIKE … INCLUDING INDEXES` |
| `src/backend/catalog/system_views.sql` | `pg_stat_*_indexes` include global indexes |
| `src/backend/utils/misc/guc_parameters.dat`, `guc_tables.c` | `dbblue_auto_global_index` |

---

## 8. Known bugs (found 2026-09-30 / 2026-10-01; re-audit 2026-10-05: items 11–18)

Severity is from an Odoo production point of view.

| # | Severity | Bug | Repro | Cause, and fix direction |
|---|---|---|---|---|
| 1 | **Fixed** | Rolling back DETACH, DROP partition or TRUNCATE (of a partition or the parent) **deletes index entries permanently** | `BEGIN; TRUNCATE part; ROLLBACK;` → entries 3000 → 1906, lookups miss rows, uniqueness lost. `TRUNCATE parent` + rollback → 0 entries | `IndexGlobalDetachPartition` bulk-deletes immediately, which is not transactional. **Fixed 2026-10-01**: the index is rewritten into new storage, which a rollback discards (§5.6). Regression test `global_partition_index` |
| 2 | **Fixed** | A failed or rolled-back ATTACH leaves its backfilled entries behind. Re-attaching duplicates them; if it's never re-attached they become permanent orphans | attach fails or rolls back, then attach again → the row is returned twice, `bt_index_check`: "item order invariant violated" | `IndexGlobalAttachPartition` inserts without first purging. **Fixed 2026-10-01**: same rewrite; a failed or rolled-back ATTACH leaves nothing, and a re-attach drops leftovers first |
| 3 | **Fixed** | `ALTER TABLE … ALTER COLUMN … TYPE` on a column of a global index **when no rewrite is needed** (e.g. `varchar(50)` → `varchar(80)`) crashes the backend on cassert builds (all sessions reset). On release builds it reads out of bounds | `ALTER TABLE d ALTER COLUMN code TYPE varchar(80);` → `TRAP: Assert("old_natts == numberOfAttributes")`, `indexcmds.c:296` | **Fixed 2026-10-01**: `CheckIndexCompatible` compares only `IndexGlobalNumUserKeys()` columns (and rebuilds instead of asserting if the counts differ). A compatible change reuses the index storage. Covered by the regression test |
| 4 | **Fixed** | **pg_dump loses global UNIQUE constraints.** It writes `ADD CONSTRAINT … UNIQUE (name, company_id, create_date)`, so the restore creates a per-partition constraint and cross-partition uniqueness is silently gone | dump + restore → `sale_order_name_uniq` is `UNIQUE (name, company_id, create_date)`, `indglobal = f` | **Fixed 2026-10-01**: `getIndexes` reads the constraint's own column count (`array_length(pg_constraint.conkey, 1)`) and `dumpConstraint` lists only those columns, so the dump says `UNIQUE (name, company_id)`; on restore DBblue auto-converts it to a valid global index again. Output for ordinary constraints is byte-identical. A database restored from an older dump keeps an invalid, empty constraint: find it with `SELECT conrelid::regclass, conname FROM pg_constraint c JOIN pg_index i ON i.indexrelid = c.conindid WHERE NOT i.indisvalid`, then drop and re-add it |
| 5 | **Fixed** | `INSERT … ON CONFLICT DO NOTHING` (no target) raises `unique_violation` on a global-index conflict | see §6 | **Fixed 2026-10-01**: global indexes are ON CONFLICT arbiters (see §5.3a). Concurrency verified: 8 sessions × 500 upserts on 50 keys, 0 errors, 0 deadlocks, no lost updates |
| 6 | Medium | Concurrent inserts of the same key can fail with `deadlock_detected` instead of `unique_violation`, and each one costs `deadlock_timeout` | 8 sessions × 400 inserts on 300 keys: 6 deadlocks, 6.2 s vs 0.2 s for a plain table. No duplicates | insert-then-check design (§5.3). Apps that retry only on unique_violation won't retry. Does not apply to `INSERT … ON CONFLICT`, whose speculative insertion reports a conflict without waiting |
| 7 | **Fixed** | `dbblue_partition_undo` drops unique indexes that Odoo added after partitioning | undo after the Odoo-upgrade flow → only the conversion-time indexes come back | **Fixed 2026-10-05** in `dbblue_partition` 1.1 (`dbblue_partition--1.0--1.1.sql`): undo records the indexes and unique/PK/exclusion constraints that exist on the partitioned table but not on the backup, and recreates them on the restored table as ordinary objects (constraints as constraints, `GLOBAL` / `ON ONLY` dropped), with their comments. The conversion's own `_fkuq` indexes are skipped. Existing databases: `ALTER EXTENSION dbblue_partition UPDATE` |
| 8 | Limitation | UPDATE/DELETE/FOR UPDATE by a globally indexed column seq-scan every partition | `EXPLAIN UPDATE d … WHERE code = 'x'` | **Not a bug** (planner restriction by design, see §5.5 and §6): results are correct; Odoo writes and locks by `id`. For custom SQL: add a normal index on the column, or `UPDATE … WHERE id IN (SELECT id … WHERE col = …)` |
| 9 | **Fixed** | Confusing messages: CONCURRENTLY prints the auto-convert NOTICE and then fails; a hash GLOBAL index says "does not support multicolumn indexes"; a failed ATTACH says "could not create unique index" | — | **Fixed 2026-10-05**: `CONCURRENTLY` is no longer auto-converted, so it gets upstream's "cannot create index on partitioned table … concurrently"; non-btree GLOBAL is rejected before the partition key is appended ("only supported for btree"); a duplicate during ATTACH says "cannot attach partition …: duplicate key value violates unique constraint …" and names the partition holding it (`GlobalUniqueCheckContext` in `ExecCheckGlobalIndexUnique`) |
| 10 | **Fixed** | `\d` doesn't show GLOBAL; `reltuples` stays -1; stale comments (`INCLUDE'd partition key` in `nodeIndexscan.c`/`allpaths.c`, "starts empty" in `indexcmds.c:1371`) | — | **Fixed 2026-10-05**: `\d` tags global indexes with `GLOBAL` (psql reads `indglobal` through `to_jsonb()`, so it still works against upstream servers); ANALYZE of the partitioned table sets the global indexes' `relpages`/`reltuples` (scaled by the predicate for partial ones); VACUUM of a partition records the exact entry count from the global cleanup scan, taking the parent's lock only if it is free; stale comments rewritten |
| 11 | ~~High~~ **Fixed** (was wrong results; Low exposure: dbblue_partition / pg_partman create partitions with `ATTACH PARTITION`, which already cleaned this up) | A new partition inherited stale entries. A dead row's entry left in the DEFAULT partition (not yet vacuumed), or an orphan left by a partition dropped through `DROP … CASCADE`/`DROP OWNED`, started routing to a partition later created for that key range, and matched whatever row lived at the same TID there | `CREATE TABLE p24 PARTITION OF t FOR VALUES …` next to a DEFAULT that had a deleted 2024 row → a range query through the global index returned the same row **twice** | **Fixed:** `DefineRelation` (tablecmds.c) now calls `IndexGlobalAttachPartition(parent, rel)` when the parent has a global index. The new partition is empty, so every entry that routes to it is stale and gets dropped by the transactional rewrite (rolled back with the CREATE). Regression test added. Remaining rare edge: an orphan of a dependency-dropped partition routes to DEFAULT (and can shadow a DEFAULT row at the same TID) until its range gets a partition again or the index is reindexed |
| 12 | ~~High~~ **Fixed** (not reachable from Odoo) | `ALTER TABLE parent SET ACCESS METHOD …, ADD CONSTRAINT … UNIQUE (…)` in one statement left the new global index **empty**. Upstream PostgreSQL 19beta2 has the same bug for ordinary partitioned indexes: their per-partition indexes were created with no storage at all (`could not read blocks 0..0`) | 1001 rows, 0 entries; an existing duplicate was not detected; lookups returned nothing | `SET ACCESS METHOD` on a partitioned table only changes its catalog row and rewrites no partition, but `ATExecAddIndex` still saw `tab->rewrite > 0` and skipped the build (global: `INDEX_CREATE_GLOBAL_NOFILL`). **Fixed:** for a partitioned table, `ATExecAddIndex` ignores `AT_REWRITE_ACCESS_METHOD` when deciding `skip_build`, so the index is built right away. Combined with a real rewrite (e.g. `ALTER COLUMN TYPE`) the rewrite still fills it. Regression test added |
| 13 | ~~High~~ **Fixed** (not reachable from Odoo; a DBA could hit it with `SET UNLOGGED` for a bulk load) | UNLOGGED partition under a logged parent: after a crash the partition was emptied but the global index kept all its entries (2000 entries for 1094 rows), until the partition was rewritten or the index reindexed | crash test | **Fixed:** unlogged partitions are refused under a parent with a global index, like sub-partitioned and foreign ones: `CREATE UNLOGGED TABLE … PARTITION OF`, `ATTACH PARTITION` of an unlogged table, `ALTER TABLE partition SET UNLOGGED`, and `CREATE … GLOBAL` index while one exists. The partition checks (`gpi_check_partition_supported`) now also run on the `INDEX_CREATE_GLOBAL_NOFILL` path, which skipped them before (it accepted a sub-partitioned tree). `SET LOGGED` stays allowed. Regression test added |
| 14 | ~~Medium~~ **Fixed** (operations) | Debug logging left in the planner: `elog(LOG, "saved global index path: %s", nodeToString(…))` runs on **every** planning of a query on a table with a global index | 144 such lines in this test run | **Fixed:** the debug loop in `apply_scanjoin_target_to_paths` (planner.c) was removed. No other debug logging remains in the global index code |
| 15 | ~~Medium~~ **Fixed** (Low: not reachable from Odoo, whose ORM always inserts into the partitioned parent; only scripts writing directly to a partition could hit it) | `INSERT INTO <partition> … ON CONFLICT DO NOTHING` (no target) failed with an internal error when the duplicate was in a sibling partition | `ERROR: XX000: ON CONFLICT through a global index requires tuple routing` | A direct insert into a leaf has no root to route the conflicting row through, and `ExecGetGlobalConflictPartition` needed routing to get the sibling's ResultRelInfo. **Fixed:** without routing it now uses `ExecGetTriggerResultRel()` for the sibling. Only DO NOTHING without a target can get there (a conflict target on a leaf never matches the parent's global index: `42P10`), and DO NOTHING needs the sibling only for the visibility check, so the row is skipped. At REPEATABLE READ it behaves as upstream does. Regression test added |
| 16 | Low | REINDEX (and CREATE INDEX) while a `DETACH CONCURRENTLY` is pending leaves out that partition's rows (3000 → 2192 entries) | consistent again after `DETACH … FINALIZE` | `build_global_index` omits detach-pending partitions while everything else still treats them as indexed |
| 17 | ~~Low~~ **Fixed** (not used by Odoo; DBA maintenance only) | `REINDEX TABLE/SCHEMA/DATABASE` (with or without CONCURRENTLY) silently skipped global indexes, because they only process the partitions; `CLUSTER parent USING <global index>` reported success but reordered nothing | Index file unchanged after each command; no message | **Fixed:** without CONCURRENTLY, `REINDEX TABLE/SCHEMA/DATABASE` now rebuild the parent's global indexes (`ReindexAddGlobalIndexes` in indexcmds.c, one transaction per index like the partitions; `REINDEX SYSTEM` leaves them alone). With CONCURRENTLY they print `WARNING: cannot reindex global index "…" concurrently, skipping` + `HINT: Use REINDEX INDEX without CONCURRENTLY.` `CLUSTER`/`REPACK … USING INDEX` on a global index now fails with `cannot cluster on global index` (`check_index_is_clusterable` in repack.c). Regression test added |
| 18 | **Not a bug** (moved to §6 "Concurrency risks") | Possible lock-order deadlocks between DML and partition DDL, a possible ON CONFLICT livelock, and SERIALIZABLE predicate locks moved to the parent on a rewrite | 2 deadlock scenarios and a 16-session single-key ON CONFLICT run did not reproduce anything | From code review. A deadlock is detected and resolved by PostgreSQL (no damage, Odoo retries); the livelock never occurred; the SSI gap needs SERIALIZABLE transactions, which neither Odoo (REPEATABLE READ) nor dbblue_partition use |
| 19 | ~~High~~ **Fixed** (regression, found by the baseline comparison on 2026-10-08; affects **every** partitioned table, with or without a global index) | `DROP TABLE`, `VACUUM FULL`/`CLUSTER`/`REPACK` and rewriting `ALTER TABLE` of a partition whose `DETACH … CONCURRENTLY` was interrupted failed with `relation … has no parent because it's being detached` | Upstream isolation tests `detach-partition-concurrently-3`/`-4` failed (teardown could not drop the pending partition); they pass on the pre-feature baseline `98f656aa19` | The global-index hooks (`RemoveRelations`, `IndexGlobalResyncPartition(s)`, the #13 `SET UNLOGGED` check) called `get_partition_parent(…, false)`, which refuses detach-pending partitions, before even checking for a global index. **Fixed:** they pass `true` (`even_if_detached`), consistent with the design (a pending partition keeps its entries until FINALIZE). Verified: the 4 upstream detach tests pass; with a global index, VACUUM FULL keeps and DROP purges the pending partition's entries (entries = rows, amcheck OK) |

Bugs 1–4 are fixed. On a database that ran an older build, `REINDEX INDEX <global index>` removes damage they may have left (REINDEX rebuilds from the partitions' heaps).

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
| G5 lifecycle | 8 / 17 → **27 / 27** after the fix | create/attach/detach/detach concurrently/drop/truncate, their rollbacks, savepoints, ATTACH retry, ALTER/CLUSTER rollbacks |
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
| DETACH a 41k-row partition | 561 ms (was 188 ms before the rollback fix) | 1 ms | rewrites the whole index |
| ATTACH it back | 907 ms (was 463 ms) | 10 ms | rewrite + uniqueness check |

### How to verify an index yourself

```sql
CREATE EXTENSION pageinspect; CREATE EXTENSION amcheck;
SELECT bt_index_check('sale_order_name_uniq');           -- structure
SELECT allequalimage FROM bt_metap('sale_order_name_uniq'); -- false = dedup off
-- entries vs rows: count leaf items with bt_page_items (skip the high key);
-- run VACUUM (INDEX_CLEANUP ON) first, because plain VACUUM may bypass index cleanup.
```

Regression test: `src/test/regress/sql/global_partition_index.sql` (in `parallel_schedule`). To run it alone against a running cluster:
`./pg_regress --bindir=<bin> --host=… --port=… --inputdir=. global_partition_index` from `src/test/regress`.

Build and test notes for this tree: it has no header dependency tracking, so
do a clean backend rebuild after editing any `.h`. `make check` fails because
initdb preloads contrib libraries, so test with `installcheck` against a
scratch cluster.
