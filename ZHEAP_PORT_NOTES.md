# Zheap → PostgreSQL 19 Port Notes

Source: `/home/cybrosys/zheap/` (PG 12 beta1 snapshot).
Target: this tree (PG 19devel).

---

## 1. What zheap is

A replacement table access method that performs **in-place UPDATE** and
keeps prior row versions in a **separate undo log**, instead of leaving
dead tuples in the heap for vacuum to reap later.

Three wins:
1. **No bloat from UPDATE** — space is reclaimed at commit time, not at vacuum time.
2. **Smaller tuple header** — 5 bytes vs heap's 24, because transaction
   metadata moves out of the tuple and into per-page transaction *slots*.
3. **Less write amplification** — in-place updates don't touch unchanged
   indexes.

Trade-off: massive new subsystem (undo logs) and a state machine in
`xact.c` that must apply undo on abort.

---

## 2. Storage layout cheatsheet

### Page (`zpage.c`, `zheap.h`)
- Looks like a heap page, but the *special space* at end of page holds a
  fixed array of **4 transaction slots** (`ZHEAP_PAGE_TRANS_SLOTS`).
- Each slot = `{FullTransactionId xid, UndoRecPtr urec}` = 16 bytes.
- Overflow beyond 4 concurrent writers → **TPD** (see §6).

### Tuple (`ztuple.c`, `zhtup.h`)
- Header = 5 bytes (`t_infomask2`, `t_infomask`, `t_hoff`).
- **No CTID** — version chain lives in undo, not in forward pointers.
- `t_infomask2` stores the **slot index** (0..3 or TPD pointer).
- Flags: `ZHEAP_DELETED`, `ZHEAP_INPLACE_UPDATED`, `ZHEAP_UPDATED`,
  `ZHEAP_LOCK_MASK`, etc.

### UndoRecPtr (`undolog.h`)
- 64-bit: `[24-bit log#][40-bit offset]` → up to 16M logs, 1TB each.
- Logs are 1MB segments under `PGDATA/base/undo/`.
- Meta-data checkpointed to `pg_undo/<lsn>` for fast startup.

---

## 3. Undo subsystem (~9.6k LOC, 9 files)

| File | LOC | Role |
|---|---|---|
| `undolog.c` | 2728 | Log allocation, segment files, head/tail pointers |
| `undoinsert.c` | ~1500 | Pack & insert undo records |
| `undorecord.c` | 514 | Serialize/deserialize record headers |
| `undoaction.c` | 561 | Apply undo (rollback replay) |
| `undodiscard.c` | 356 | Free old undo (xid < GlobalXmin) |
| `undoworker.c` | 821 | Launcher + worker bgworker entry points |
| `undorequest.c` | 1488 | Pending-rollback request queue |
| `discardworker.c` | 196 | Helper for discard |
| `undoactionxlog.c` | 49 | WAL replay for undo application |

**Three pointers per log:**
- `insert` — next byte to write (head)
- `discard` — oldest byte still needed (tail)
- `end` — high-water of allocated space

**Each backend owns one log** while active → no contention on the head pointer.

**Pages live in the shared buffer pool**, but with an alternate SMGR
(`undofile.c`) so WAL doesn't need full-page images.

---

## 4. Undo record types

`UNDO_INSERT`, `UNDO_MULTI_INSERT`, `UNDO_DELETE`, `UNDO_INPLACE_UPDATE`,
`UNDO_UPDATE`, `UNDO_XID_LOCK_ONLY`, `UNDO_XID_LOCK_FOR_UPDATE`,
`UNDO_XID_MULTI_LOCK_ONLY`, `UNDO_ITEMID_UNUSED`.

Header is packed (no padding): rmgr, type, info-flags, prev-record-len,
relation OID, prev-xid, cur-xid, cid. Optional sections follow per flag:
relation details, block, transaction (epoch + prev/next log links),
payload (old/new tuple bytes).

Records chain backwards via `urec_prevlen` for reverse traversal.

---

## 5. MVCC visibility (`zheapam_visibility.c`, 62K)

Reader algorithm:
1. Load tuple, read slot index from `t_infomask2`.
2. From page's slot array: get `(xid, urec)`.
3. Apply snapshot test on `xid`.
4. If invisible: fetch prior tuple from undo via `urec`.
5. Repeat from step 3 with the undo record's embedded xid (NOT the slot's
   — the slot may have been reused).
6. Walk until a visible version is found or the chain ends at insertion.

**No freezing needed** — slot stores `FullTransactionId` (epoch+xid), and
once `xid < oldestXidWithEpochHavingUndo` the chain is implicitly all-visible.

---

## 6. TPD — Transaction Page Directory (`tpd.c`, 95K)

Solves the 4-slot deadlock: if 5+ txns want the same page concurrently,
TPD allocates an overflow page with extra slots. Sequential scans know
to skip TPD pages. A meta-page (block 0) tracks live TPD entries.
Pruned lazily when their oldest xid is all-visible (`prunetpd.c`).

---

## 7. Workers

- **UndoLauncher** — bgworker, polls request queue, spawns workers up to
  `max_undo_workers` (default 5).
- **UndoWorker** — applies rollback batches per page; calls
  `process_and_execute_undo_actions_page()`.
- **Discard worker logic** — runs inside the worker; truncates logs once
  their oldest xid < GlobalXmin.

Registered from postmaster startup like autovacuum/apply launcher.

---

## 8. Core files zheap modifies (the integration cost)

These are the **hooks into core** — every one must be re-applied against
the current PG19 versions of these files. These files have all had heavy
refactoring 12 → 19.

| Core file | What zheap adds |
|---|---|
| `access/transam/xact.c` | `TransactionState.performUndoActions`, states `TRANS_UNDO`/`TBLOCK_UNDO`/`TBLOCK_SUBUNDO`, `SetUndoActionsInfo()`, `ApplyUndoActions()` called from every abort path |
| `access/transam/twophase.c` | Store undo pointers in 2PC state file; replay on prepared-txn rollback |
| `access/transam/xlog.c` | `StartupUndoLogs()` during recovery; read `pg_undo/<lsn>` meta files |
| `storage/smgr/smgr.c` | Register `undofile` as a new SMGR (`SYNC_HANDLER_UNDO`) |
| `storage/smgr/undofile.c` | NEW (~500 LOC) — SMGR impl: open/read/write/writeback/unlink |
| `include/access/xact.h` | Export undo state + functions |

---

## 9. PG12 → PG19 port pain points (specifics to watch for)

1. **`TableAmRoutine` struct** — fields added/removed in 13, 14, 17, 18.
   `zheapam_handler.c` will not compile until every callback signature is
   re-checked against the current definition in
   `src/include/access/tableam.h`.
2. **Snapshot API** — `Snapshot` struct fields and `HeapTupleSatisfies*`
   names changed; zheap's visibility code is a near-clone of the heap one.
3. **Buffer manager** — `ReadBufferExtended` flags, `ReadBufferBI`,
   pinning semantics evolved. Used heavily in `zheapam.c`, `zundo.c`.
4. **WAL helpers** — `XLogRegisterBuffer`, `XLogRegisterBufData`,
   record-builder macros tightened. All of `zheapamxlog.c` (60K) and
   `tpdxlog.c` need re-validation.
5. **Vacuum API** — `VacuumParams` and the `LVRelState` machinery have
   been heavily refactored. `zvacuumlazy.c` (45K) is at risk.
6. **`FullTransactionId`** — exists in PG19, but helper macros may have
   moved. Zheap relies on it pervasively.
7. **`xact.c` callback registration** — register-on-commit /
   register-on-abort hook lists have changed shape.
8. **`BackgroundWorker` registration** — `bgw_library_name="postgres"`
   workers must be added to `InternalBGWorkers[]` (you hit this exact
   issue with the auto-backup worker today).
9. **SMGR API** — `f_smgr` callback list has been extended; `undofile.c`
   will need new entries (e.g., `smgr_zero_damaged_pages`,
   `smgr_writev`).
10. **LWLock tranche registration** — `LWLockNewTrancheId()` /
    `LWLockRegisterTranche()` shape changes; zheap registers
    `discard_lock` and `discard_update_lock`.

---

## 10. File inventory

### Zheap (17 files, ~750K)
| File | Size | Role |
|---|---|---|
| `zheapam.c` | 271K | DML core (insert/update/delete/scan/fetch/lock) |
| `tpd.c` | 95K | TPD overflow slots |
| `zheapam_visibility.c` | 62K | Visibility via undo |
| `zheapam_handler.c` | 61K | TableAmRoutine dispatch |
| `zheapamxlog.c` | 60K | WAL builders & redo |
| `zvacuumlazy.c` | 45K | Vacuum |
| `zundo.c` | 42K | Apply undo, reconstruct tuples |
| `zscan.c` | 41K | Seq/bitmap/sample scans |
| `prunezheap.c` | 29K | Page-level pruning |
| `ztuptoaster.c` | 28K | TOAST for zheap |
| `ztuple.c` | 27K | Tuple header accessors |
| `zmultilocker.c` | 21K | Multi-txn lock tracking |
| `zpage.c` | 17K | Page alloc, item placement |
| `zhio.c` | 16K | Buffer I/O helpers |
| `prunetpd.c` | 14K | TPD pruning |
| `tpdxlog.c` | 14K | TPD WAL |
| `rewritezheap.c` | 12K | CLUSTER / rewrite |

### Undo (9 files, ~236K) — see §3

### New SMGR (1 file)
- `storage/smgr/undofile.c` — ~500 LOC

### New headers (11)
`access/zheap.h`, `access/zhtup.h`, `access/undolog.h`,
`access/undorecord.h`, `access/undoworker.h`, `access/undodiscard.h`,
`access/undorequest.h`, `access/tpd.h`, `access/tpd_xlog.h`,
`storage/undofile.h`, plus an `undoinsert.h`.

---

## 11. Recommended port order (one engineer, weeks)

| # | Module | Weeks | Why this order |
|---|---|---|---|
| 1 | Undo log core (`undolog.c`, `undorecord.c`, `undoinsert.c`) | 3 | Lowest-level dependency for everything |
| 2 | Core hooks in `xact.c` / `twophase.c` / `xlog.c` | 2 | Zheap DML calls into these |
| 3 | Undo workers + request queue | 1 | Needed for automatic rollback/discard |
| 4 | `undofile.c` SMGR integration | 0.5 | Self-contained, small |
| 5 | Tuple + visibility (`ztuple.c`, `zheapam_visibility.c`, `zundo.c`) | 4 | Most delicate logic; test in isolation |
| 6 | Page format + TPD (`zpage.c`, `tpd.c`, `tpdxlog.c`, `prunetpd.c`) | 3 | Mid-level deps |
| 7 | DML (`zheapam.c`, `zheapam_handler.c`) | 6 | Biggest single file (271K); start once 1-6 stable |
| 8 | Scans + vacuum + rewrite | 4 | Test with simple INSERT/SELECT first |
| 9 | Utilities (`zhio`, `zmultilocker`, `ztuptoaster`, `zheapamxlog`) | 2 | Tail dependencies |
| 10 | Regression + perf | 3 | |
| **Total** | | **~28 weeks** | for one engineer |

---

## 12. What "next session" should look like if you want to start

1. New branch: `git checkout master && git checkout -b feature/zheap_am`
2. Stage Phase 0 — copy files into the PG19 layout, register Makefiles
   + `meson.build`, but wrap each new `.c` in `#if 0 / #endif` so the
   tree still builds.
3. Commit "zheap: stage source from PG12 reference, all modules disabled".
4. Pick **one** module from the table above and unblock it module-by-module.

Do **not** try to unblock everything at once — you will lose the ability
to bisect.

---

## 13. Honest assessment

- The undo-log subsystem zheap depends on **never merged upstream** and
  has had no maintenance since 2020. You are reviving abandoned code.
- For an Odoo-specific PG, the workload is INSERT-heavy with bursty
  UPDATEs on a few hot rows (e.g., `ir_sequence`, stock moves). Zheap's
  in-place UPDATE wins matter most here, but a lot of the bloat pain on
  Odoo is fixable with cheaper changes:
  - aggressive autovacuum on hot tables
  - `fillfactor` tuning (your `feature/custom_fillfactor` branch already
    explores this)
  - HOT-eligible UPDATEs (avoid touching indexed columns)
- Consider whether the 6-month port is worth it vs. those cheaper wins
  on your actual workload. Run pgbench-style measurements on a sample
  Odoo workload **before** committing to the port.
