# Auto-partition launcher (DBblue)

A built-in background worker that automatically converts heap tables to
range-partitioned tables, rotates new partitions, and detaches old ones
beyond a user-configured retention.  Designed for high-churn Odoo
tables (`mail_message`, `bus_bus`, `account_move_line`,
`stock_move_line`, etc.) where the operator wants partition lifecycle
to be a one-time `ALTER TABLE` rather than a custom cron.

The feature has zero shared-library or `shared_preload_libraries`
requirement — it lives in the postgres binary and starts on every
DBblue server.

------------------------------------------------------------------------
## User interface

### Reloptions (per table)

```sql
ALTER TABLE my_table SET (
    auto_partition_strategy = 'range_int' | 'range_date' | 'list_int' | 'off',
    auto_partition_column   = '<column name>',
    auto_partition_interval = '<rows>'    -- range_int
                            | '<interval>',  -- range_date, e.g. '1 month'
    auto_partition_retention = <int>      -- 0 = unlimited
);
```

### Server GUCs (live in `guc_parameters.dat`, survive `ALTER SYSTEM` + restart)

| GUC | Default | What it does |
|---|---|---|
| `auto_partition_enabled` | `true` | Master switch; worker idles when off |
| `auto_partition_naptime` | `60s` | Seconds between launcher iterations |

### Operational SQL (via `dbblue_admin` extension)

```sql
CREATE EXTENSION dbblue_admin;
SELECT * FROM dbblue_auto_partition;
-- schemaname | relname | strategy | partition_column | partition_interval | retention
```

------------------------------------------------------------------------
## End-to-end flow

```sql
-- A populated heap, with FKs in/out, indexes, and a sequence-driven id
CREATE TABLE mail_message (
    id        bigserial PRIMARY KEY,
    author_id int REFERENCES res_partner(id),
    body      text);
-- ... data accumulates ...

ALTER TABLE mail_message SET (
    auto_partition_strategy = 'range_int',
    auto_partition_column   = 'id',
    auto_partition_interval = '5000000',
    auto_partition_retention = 24);

-- Within one launcher iteration:
--   1. Capture all FKs (in + out)
--   2. Drop FKs
--   3. CREATE TABLE __new (LIKE INCLUDING ALL) PARTITION BY RANGE(id)
--   4. CREATE initial partition spanning [0, max(id) + interval)
--   5. INSERT INTO __new SELECT * FROM mail_message  (no lock yet)
--   6. Detach owned sequences
--   7. ACCESS EXCLUSIVE on source -> DROP -> RENAME __new (sub-second)
--   8. Re-attach sequences
--   9. Re-add FKs as NOT VALID
--  10. Lock released
--  11. VALIDATE each FK (ShareUpdateExclusiveLock; concurrent writes OK)
--
-- Subsequent iterations:
--   * If head partition is >= 80% full, create the next one
--   * If partition_count > retention, DETACH + DROP the oldest
```

------------------------------------------------------------------------
## What's implemented (12 commits on `feature/auto-partition`)

### Coverage matrix

| Strategy | Heap → partitioned | FKs preserved | Triggers preserved | Rotation | Retention | Fill-gate |
|---|---|---|---|---|---|---|
| `range_int`  | ✅ | ✅ both directions | ✅ | ✅ | ✅ | ✅ 80% |
| `range_date` | ✅ | ❌ refused (composite-PK FK shape) | ❌ refused (same reason) | ✅ | ✅ | ✅ 80% |
| `list_int`   | ❌ TODO | — | — | ❌ TODO | n/a | n/a |

### What survives across conversion (range_int)

- All indexes (`LIKE INCLUDING ALL` plus PG's auto-propagation to partitions)
- The PRIMARY KEY constraint
- The owned sequence backing `bigserial` / `serial` (detached + re-attached)
- All foreign key constraints in both directions, with their original names
  (re-added as `NOT VALID`, validated post-swap under ShareUpdateExclusiveLock)
- All user triggers on the source — captured via `pg_get_triggerdef`,
  recreated post-swap on the partitioned parent (PG auto-propagates to
  partitions).  Includes the BEFORE/AFTER ROW triggers Odoo's
  `tracking=True` modules generate.
- All `DEFAULT` clauses, generated columns, comments, statistics targets
- The auto-partition reloptions themselves (re-applied post-swap)

### Production-grade properties

- **Sub-second swap window** regardless of table size — FK validation moved
  out of the ACCESS EXCLUSIVE lock to a post-swap pass under
  `ShareUpdateExclusiveLock` (concurrent SELECT/INSERT/UPDATE proceed).
- **Subtransaction-isolated** — failure in any one relation's processing
  leaves the source heap untouched and the launcher retries next iteration.
- **Crash-safe** — bgworker restarts after 30s on crash, picks up where it
  left off.
- **80% fill heuristic** — empty/half-full head partitions don't churn
  pg_class with new shells.
- **GUCs survive restart** — `ALTER SYSTEM SET` works correctly because the
  GUCs are registered in `guc_parameters.dat`, not via `DefineCustomXxx`
  at runtime.

------------------------------------------------------------------------
## What's NOT implemented yet

In rough priority order:

### range_date FK + trigger handling
**Currently:** `convert_heap_to_partitioned_date()` still refuses FKs
and triggers because date-partitioning typically requires a composite
PK `(id, partition_column)`, and existing FKs that reference just
`id` won't match the new unique constraint shape.
**Need:** either (a) require application cooperation (caller must
update referencing FKs to include the partition column), or (b)
generate a separate non-partition-key UNIQUE INDEX on `id` to keep
FKs working unchanged.  Option (b) is preferable but requires careful
analysis of write hot-path overhead.  Once FKs work, lifting the
trigger refusal is mechanical.
**Estimated:** ~300 lines plus design discussion.

### `list_int` strategy
**Currently:** declared in the reloption enum, not implemented in the
launcher.
**Use case:** multi-tenant Odoo schemas where each `company_id` should
live in its own partition.  Different mechanics from range — partitions
are added on-demand when previously-unseen values appear, not on a
periodic clock.
**Estimated:** ~400 lines (rotation + heap conversion + retention
semantics: how do you "expire" a company partition?).

### `pg_stat_auto_partition` view
**Currently:** the `dbblue_auto_partition` view in the dbblue_admin
extension shows configured tables but not their per-partition state.
**Need:** SRF that returns one row per (parent, partition) with
created-at, row-count estimate, last-rotation timestamp, fill ratio.
**Estimated:** ~200 lines, mostly a SQL view over pg_inherits +
pg_class + a small bit of state in shmem.

### Online conversion via logical replication
**Currently:** the data copy runs while writers proceed normally, but
the rename swap still needs ACCESS EXCLUSIVE briefly (sub-second on
small tables, longer on giant tables because of catalog cache
invalidation).
**Need:** pg_repack-style live capture — set up a logical replication
slot on the source, copy in batches, drain captured changes, atomic
swap.
**Estimated:** ~1500 lines.  Largest remaining piece.

### `ADD CONSTRAINT NOT VALID` parallelism for FKs
**Currently:** the post-swap VALIDATE loop runs FKs serially.  For a
table with 10 inbound FKs each scanning a million-row referrer, that's
serialized minutes of validation.
**Need:** either parallelize via a worker pool, or skip VALIDATE and
let the operator run it on their own schedule.
**Estimated:** ~100 lines.

### Refactoring shared helpers
**Currently:** `convert_heap_to_partitioned()` and
`convert_heap_to_partitioned_date()` duplicate the prereq checks,
sequence detach/reattach dance, and FK capture/restore.
**Want:** factor the shared ~400 lines into helpers so the next
strategy implementation is small.
**Estimated:** ~150 lines saved net, code-health win.

------------------------------------------------------------------------
## Known limitations / gotchas

- **Brief ACCESS EXCLUSIVE during swap.**  Even with NOT VALID, the
  RENAME step needs an exclusive lock for catalog consistency.  This
  is constant time (sub-second on every table size we've tested) but
  is a real lock.  Phase 3d (online via logical replication) eliminates
  it.

- **`partitioned_table_reloptions` allows only auto_partition_*.**
  Stock PG reloptions like `fillfactor` cannot be set on the partitioned
  parent (PG limitation, unrelated to DBblue).  Set them on partitions
  directly.

- **`pg_relation_size` and friends may return stale answers** on the
  development tree we built atop — those views inline SQL that depends
  on serialized node trees, and partial rebuilds drift.  Production
  installs from a clean initdb don't have this issue.  `pg_class.relpages * 8`
  is a reliable workaround.

- **80% fill threshold is hardcoded.**  Customers wanting a different
  cadence (50% to be aggressive about always having empty headroom,
  95% to minimize partition count) need a code change.  Making this
  a reloption is a small follow-up.

- **GUCs default to "postgres" database** — the launcher connects to
  the database literally named `postgres` and only scans relations
  there.  Multi-DB clusters need either one launcher per database
  (dynamic registration) or a way to scan across databases.  The
  current single-DB approach matches Odoo's typical "one DB per
  customer" deployment shape.

- **Multi-column PKs are refused for range_int conversion.**  PK must
  equal the partition column.  Loosening this requires additional
  shape analysis.

------------------------------------------------------------------------
## Source layout

```
src/backend/postmaster/autopartition.c   -- the launcher worker
src/include/postmaster/autopartition.h   -- declarations
src/backend/postmaster/postmaster.c      -- AutoPartitionLauncherRegister() call
src/backend/postmaster/bgworker.c        -- InternalBGWorkers entry
src/backend/access/common/reloptions.c   -- reloption registration + validation
src/include/utils/rel.h                  -- StdRdOptions fields + helper macros
src/backend/utils/misc/guc_parameters.dat -- GUC entries
src/backend/utils/misc/guc_tables.c      -- include header for the externs
contrib/dbblue_admin/                    -- ops view + status function
```

About 2200 lines of new C across these files.

------------------------------------------------------------------------
## Reproducing the demo

```sql
-- range_int with FKs
DROP TABLE IF EXISTS mail_followers, mail_msg, res_partner_test CASCADE;
CREATE TABLE res_partner_test (id bigserial PRIMARY KEY, name text);
CREATE TABLE mail_msg (
    id bigserial PRIMARY KEY,
    author_id int REFERENCES res_partner_test(id),
    body text);
CREATE TABLE mail_followers (
    id bigserial PRIMARY KEY,
    msg_id int NOT NULL REFERENCES mail_msg(id),
    follower int);

INSERT INTO res_partner_test(name)
    SELECT 'p'||i FROM generate_series(1, 1000) i;
INSERT INTO mail_msg(author_id, body)
    SELECT 1 + (random()*999)::int, 'msg'
    FROM generate_series(1, 5000) i;
INSERT INTO mail_followers(msg_id, follower)
    SELECT 1 + (random()*4999)::int, (random()*99)::int
    FROM generate_series(1, 8000) i;

-- One ALTER triggers the whole pipeline:
ALTER TABLE mail_msg SET (
    auto_partition_strategy='range_int', auto_partition_column='id',
    auto_partition_interval='2000', auto_partition_retention=12);

-- Within ~60s the launcher has converted, rotated, and validated.
-- Watch the server log for 'auto-partition: ...' lines.
SELECT * FROM dbblue_auto_partition;

-- range_date
DROP TABLE IF EXISTS stock_move_line CASCADE;
CREATE TABLE stock_move_line (
    id          bigserial NOT NULL,
    create_date timestamptz NOT NULL DEFAULT now(),
    product_qty numeric(12,2),
    PRIMARY KEY (id, create_date));
INSERT INTO stock_move_line(create_date, product_qty)
    SELECT '2026-01-01 00:00:00+00'::timestamptz + (i * interval '1 minute'),
           (random() * 100)::numeric(12,2)
    FROM generate_series(1, 50000) i;

ALTER TABLE stock_move_line SET (
    auto_partition_strategy='range_date',
    auto_partition_column='create_date',
    auto_partition_interval='1 month',
    auto_partition_retention=12);
```

------------------------------------------------------------------------
## Commit history (`feature/auto-partition`)

```
dfe0759f833  Phase 3d-trig — trigger-aware conversion
c4f7c710ea7  re-add FKs as NOT VALID, validate after swap
e3cb7d1107c  Phase 3c — FK-aware heap conversion (range_int)
e02514e2c47  Phase 3a-date — heap conversion for range_date
4efe06b6fa4  range_date rotation + fix scan-loop SPI clobber
96cbd87e18c  only create next partition when head is >= 80% full
88602974100  register GUCs in guc_parameters.dat (survives restart)
d9489bf34f1  Phase 3a — launcher converts heap to partitioned
d121d280206  Phase 2 step 3 — range_int rotation + retention enforcement
f0fea928f17  Phase 2 step 2 — built-in launcher background worker
43d37dc5e46  dbblue_admin: make PG_CONFIG overridable in PGXS branch
c8368bc5a71  Phase 2 step 1 — read-side macros + dbblue_admin extension
34fc07eb277  Phase 1 reloption scaffold
```
