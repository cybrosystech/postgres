# DBblue Columnar Engine — GUC reference

Quick reference for every `dbblue_columnar.*` setting, when it takes effect, and
how we used them (e.g. the Odoo demo on `odoo_ui`). Defaults and contexts are
from `dbblue_columnar.c` (`_PG_init`).

## Turning the engine on (one-time setup)

The engine is a preloaded module, so it must be in `shared_preload_libraries`,
and `enabled` must be on. Both need a **server restart**:

```conf
# postgresql.conf  (or: ALTER SYSTEM SET ...)
shared_preload_libraries = 'dbblue_columnar'
dbblue_columnar.enabled = on
dbblue_columnar.memory_mb = 8192          # store budget; size to your data
```
```sh
pg_ctl -D <datadir> restart
```

Then register + populate columns per table (see "Companion SQL" below). The
store is in ephemeral shared memory, so it is **empty after every restart** —
repopulate (or let the auto-refresh worker rebuild) before relying on it.

## The settings

| GUC | Type | Default | Set via / takes effect | What it does |
|-----|------|---------|------------------------|--------------|
| `dbblue_columnar.enabled` | bool | `off` | superuser `SET` / **`ALTER DATABASE`** / `ALTER SYSTEM` → reload | Master switch, **per database**. Off = engine inert for that database (no store, no paths). |
| `dbblue_columnar.memory_mb` | int (MB) | `128` (min 128) | postgresql.conf / `ALTER SYSTEM` → **restart** | **Cluster** ceiling for the column store (DSA), shared by all databases. |
| `dbblue_columnar.database_memory_mb` | int (MB) | `0` (unlimited) | superuser `SET` / **`ALTER DATABASE`** → reload | **One database's quota** within `memory_mb`. Set per tenant so one database cannot consume the memory another needs. |
| `dbblue_columnar.autorefresh_database` | string | `''` (off) | postgresql.conf / `ALTER SYSTEM` → **restart** | The one database whose registered tables the background worker auto-**populates** and maintains (see "Auto-populate" below). Empty = worker idle → you must `dbblue_columnar_populate` manually. |
| `dbblue_columnar.enable_columnar_scan` | bool | `on` | `SET` / `ALTER DATABASE` / `ALTER ROLE` → **new connections** | Lets the planner read the store. Off = engine stays loaded + store kept, but queries use heap. **Use this to A/B columnar vs heap.** |
| `dbblue_columnar.log_coverage_misses` | bool | `on` | superuser `SET` / `ALTER SYSTEM` → reload | Emits a `LOG` naming the unregistered column(s) when a query on a registered table falls back to heap. Silence = full coverage. Dedup: once per table per backend. |
| `dbblue_columnar.auto_columnarize` | bool | `off` | superuser `SET` / **`ALTER DATABASE`** → reload | Let the engine auto-pick columns to columnarize (vs manual `_add`). Not yet implemented. |
| `dbblue_columnar.naptime` | int (s) | `60` (1–86400) | superuser `SET` / **`ALTER DATABASE`** → reload | Seconds between auto-populate/refresh passes. |
| `dbblue_columnar.refresh_threshold` | int (%) | `20` (1–100) | superuser `SET` / **`ALTER DATABASE`** → reload | How far a table's all-visible page count may drift from its build-time baseline before the worker rebuilds it. |
| `dbblue_columnar.suggestions` | bool | `off` | superuser `SET` / `ALTER ROLE` / `ALTER SYSTEM` → reload | Collect workload evidence for the suggestion engine. **Independent of `enabled`** — advice can be gathered before the engine is switched on. |
| `dbblue_columnar.suggestion_slots` | int | `4096` (0–1048576) | postgresql.conf / `ALTER SYSTEM` → **restart** | Query-shape bundles the advisor can track (~144 bytes each in main shared memory). `0` disables it and reserves nothing. |
| `dbblue_columnar.suggest_min_pages` | int (pages) | `1024` (8 MB) | superuser `SET` / `ALTER SYSTEM` → reload | Smallest relation the advisor will suggest. Below this the heap is already fast enough to not be worth store budget. |

### "Takes effect" legend (PostgreSQL GUC context)
- **restart** (`PGC_POSTMASTER`): `memory_mb`, `autorefresh_database`, `suggestion_slots` — read once at server start. These are the only genuinely server-wide settings left.
- **superuser set, per database** (`PGC_SUSET`): `enabled`, `database_memory_mb`, `auto_columnarize`, `naptime`, `refresh_threshold`, `log_coverage_misses`, `suggestions`, `suggest_min_pages` — `SET` in-session, or `ALTER DATABASE db SET ...` to scope them to one database.
- **new connections** (`PGC_USERSET`): `enable_columnar_scan` — any user; `SET` in-session, or `ALTER DATABASE db SET ...` / `ALTER ROLE r SET ...` for persistence (existing/pooled connections keep the old value until they reconnect).
- **superuser set** (`PGC_SUSET`): `log_coverage_misses`, `suggestions`, `suggest_min_pages` — superuser `SET`, or `ALTER SYSTEM` + reload.

## Auto-populate via the background worker

The `autorefresh_database` / `naptime` / `refresh_threshold` GUCs configure a
background worker that does the `populate` for you. Every `naptime` seconds it
walks the **registered** tables of `autorefresh_database` and calls
`dbblue_columnar_populate` when either:
- the table **has no store version yet** — initial build, and also the rebuild
  after a restart (the store is ephemeral, so a fresh start has no version); or
- its all-visible page count has **drifted ≥ `refresh_threshold`%** from the
  build-time baseline — a refresh as the table's data changes.

So the normal flow is: `dbblue_columnar_add(...)` once (register the columns),
then the worker builds and maintains the store automatically — no manual
populate, even across restarts. Notes: it services **one** database (fixed at
startup — change requires a restart) and only **already-registered** tables
(registration is separate; automatic only if `auto_columnarize` is on).
`dbblue_columnar_populate(...)` remains the immediate/on-demand path when you
don't want to wait for the next pass.

## Companion SQL (not GUCs, but needed to use the engine)

```sql
-- register the columns to columnarize for a table
SELECT dbblue_columnar_add('account_move_line',
       ARRAY['company_id','journal_id','account_id','partner_id','move_id',
             'date','date_maturity','parent_state','display_type',
             'debit','credit','balance','amount_currency','full_reconcile_id']);

SELECT dbblue_columnar_remove('account_move_line', ARRAY['display_type']); -- inverse of _add
SELECT dbblue_columnar_populate('account_move_line');   -- build the store (also after any restart)
SELECT dbblue_columnar_drop('account_move_line');       -- drop the store for a table

-- inspect
SELECT * FROM dbblue_columnar_relations;                -- raw registration table (relid, attnum, ...)
SELECT * FROM dbblue_columnar_status;                   -- per-column status: column_name, built,
                                                        --   blocks, store_rows, store_bytes, store_size
SELECT * FROM dbblue_columnar_memory();                 -- raw: budget_mb, used_bytes, dsa_total_bytes
SELECT * FROM dbblue_columnar_memory_status;            -- readable: adds pg_size_pretty'd used /
                                                        --   dsa_total + pct_of_budget
SELECT count(*) FROM dbblue_columnar_blocks('account_move_line');   -- block count / zone maps
```

The `_status` views are the readable layer (v1.1+): `dbblue_columnar_status`
resolves the opaque `attnum` to a `column_name` and joins live store state
(`built`, `blocks`, `store_rows`, and both raw `store_bytes` + pretty
`store_size`); `dbblue_columnar_memory_status` pretties the byte counts and adds
`pct_of_budget`. The underlying `dbblue_columnar_relations` table and
`dbblue_columnar_memory()` function stay the precise primitives (raw bytes).

## The suggestion engine (advisor) — what to columnarize

Registering columns by hand means guessing, then finding out one table at a
time from `log_coverage_misses`. The advisor answers the question from the
workload instead: two planner hooks record which columns each query shape
actually reads, and a flush lands the evidence in a reviewable table.

```sql
-- 1. collect. Independent of dbblue_columnar.enabled, so you can gather advice
--    BEFORE adopting the engine. Scope it to the reporting role if you only
--    care about reports:
ALTER SYSTEM SET dbblue_columnar.suggestions = on;   SELECT pg_reload_conf();
--    or:  ALTER ROLE odoo_reports SET dbblue_columnar.suggestions = on;

-- 2. run the workload (a day of real reports beats any synthetic guess)

-- 3. drain the accumulator into the tables. The background worker does this
--    every naptime seconds when autorefresh_database is set; this works
--    anywhere, including an install with no worker configured.
SELECT dbblue_columnar_suggestions_flush();

-- 4. review, best first
SELECT relid, columns, filter_columns, score, aggregate_shape,
       selectivity, projected_size, eligibility
FROM dbblue_columnar_suggestion_status LIMIT 20;

-- 5. apply a bundle - WHOLE, never partially (see below)
SELECT dbblue_columnar_add('account_move_line',
       (SELECT columns FROM dbblue_columnar_suggestion_status
        WHERE relid = 'account_move_line'::regclass ORDER BY score DESC LIMIT 1));
```

### Apply the whole bundle or none of it

A suggestion is a **bundle**: one query shape plus the complete set of columns
it touches. A columnar scan is disqualified when *any* column it references is
unregistered, so registering 8 of a 9-column bundle buys **exactly zero**
speedup — not 8/9 of it. This is why advice is stored per bundle rather than
per column.

### Reading the output

| Column | Meaning |
|--------|---------|
| `columns` | The bundle. Pass it straight to `dbblue_columnar_add`. |
| `filter_columns` | The subset read by a `WHERE` clause — these are what make zone-map block skipping work. |
| `score` | Benefit per byte of store budget. Relative, not an absolute prediction. |
| `aggregate_shape` | Seen under a grouped aggregate, so it can reach the large `DBBlueColumnarAgg` win rather than only row-serve. |
| `selectivity` | ~1.0 = full scan; near 0 = indexed point lookup, which columnar cannot help. |
| `allvisfrac` | Fraction of pages all-visible. **The one to check before applying:** only all-visible ranges are ever built, so a churning table (`mail_message`, `stock_move`) scores low here and will not benefit however attractive the shape looks. Page-granular, so it *over*-states buildability — one modified page disqualifies its whole 32-page block. |
| `projected_size` | Estimated store footprint for the bundle. Weigh against `memory_mb`. |
| `eligibility` | `eligible`, or why this shape can *never* be accelerated: `partitioned`, `system_column`, `access_method`, `tablesample`, `lateral`, `inheritance`. |

### Why is the list empty?

`dbblue_columnar_suggestion_stats()` exists so silence is always diagnosable:

| Reading | Meaning |
|---------|---------|
| `collecting = false` | The GUC is off in the session that ran the workload. |
| `observations = 0` | The hooks saw no qualifying relation — usually `suggest_min_pages` is above your tables' size. |
| `noqueryid_drops > 0` | No query fingerprint. The module calls `EnableQueryId()`, so this only happens with `compute_query_id = off` explicitly set. |
| `contended_drops > 0` | Stripe-lock contention. Harmless (the advisor never waits on a planner), but evidence is being sampled rather than fully counted. |
| `full_drops > 0` | `suggestion_slots` is too small for the number of distinct query shapes. |

`observations` should always equal `recorded` plus the three drop counters.

### Notes
- Evidence lives in **main shared memory** and is wiped on restart, like the column store. Flush before a planned restart to keep it. Unlike registrations, suggestions are **not** dumped by `pg_dump`: they describe one server's workload, and restoring another host's advice would present evidence that was never earned there.
- The advisor costs one boolean load per planned relation when off, and one bounded, non-waiting stripe-lock acquisition when on. It never allocates and never delays a query.
- Partitioned tables are reported on the **parent** (`eligibility = 'partitioned'`); per-partition columnar acceleration is future work.

## Running several databases on one server

Everything except three settings is **per database**. On a host with more than
one database (the normal Odoo case) this matters twice over: you can enable the
engine for one database without touching the others, and you can stop one
database's store from consuming the memory another one needs.

```sql
-- turn the engine on for one tenant only; the rest stay untouched
ALTER DATABASE odoo_prod SET dbblue_columnar.enabled = on;

-- and cap what each may hold, within the server-wide memory_mb ceiling
ALTER DATABASE odoo_prod  SET dbblue_columnar.database_memory_mb = 6144;
ALTER DATABASE odoo_small SET dbblue_columnar.database_memory_mb = 512;
```

Pooled connections keep the old value until they reconnect, so **restart the
app** (Odoo) after an `ALTER DATABASE`.

### The two memory limits

| Setting | Scope | Meaning |
|---------|-------|---------|
| `memory_mb` | server (restart) | Total the column store may hold across **all** databases. |
| `database_memory_mb` | database | This database's share of it. `0` = no per-database limit. |

Both are enforced on every reservation, so a build stops at whichever binds
first. **With `database_memory_mb` left at 0, databases compete**: whichever
populates first takes what it wants, and the others get a degraded store whose
only symptom is scans quietly falling back to heap. Set a quota per tenant if
more than one database uses the engine.

A quota bounds new reservations; it does **not** evict a store already built
before the quota was lowered. Drop and repopulate to shrink an existing one.

### Which limit is stopping a build?

```sql
SELECT * FROM dbblue_columnar_database_memory_status;
```
The three denial counters have opposite fixes, and `verdict` says which applies:

| Counter | Meaning | Fix |
|---------|---------|-----|
| `denials_db_quota` | This database hit its own quota. | Raise `database_memory_mb` **for this database**. |
| `denials_cluster` | The server-wide budget is full. | Raise `memory_mb` (restart), or free some in another database. |
| `denials_no_slot` | More than 64 databases hold a store at once. | Raise `DBBC_MAX_DATABASES` and rebuild. |

`denials_db_quota` and `denials_cluster` are **per database** — you see your own,
not another tenant's. `dbblue_columnar_memory_status` remains the cluster-wide
view.

### Still server-wide
- `memory_mb` — the cluster ceiling by definition.
- `suggestion_slots` — sizes a fixed shared-memory area at postmaster start.
- `autorefresh_database` — names the single database the background worker
  services. So automatic populate/refresh and the automatic suggestion flush
  still cover **one** database; elsewhere call `dbblue_columnar_populate` and
  `dbblue_columnar_suggestions_flush()` directly. A per-database launcher is
  future work.

## Operational notes / gotchas
- **Ephemeral store.** DSA-backed, never WAL-logged, wiped on restart. Repopulate after every restart (or configure `autorefresh_database` so the worker rebuilds it).
- **A/B a query columnar vs heap:** `SET dbblue_columnar.enable_columnar_scan = off;` then run it, then `= on`. From an app with a connection pool (e.g. Odoo), use `ALTER DATABASE ... SET ...` and **restart the app** so its pooled connections reconnect.
- **Confirm routing** with `EXPLAIN`: look for `Custom Scan (DBBlueColumnarAgg)` (grouped-aggregate pushdown), `Custom Scan (DBBlueColumnarScan)` (row-serve, also the leaf of parallel plans), or `Parallel Custom Scan (DBBlueColumnarScan)`.
- **Coverage tuning:** if a report is slow, `dbblue_columnar.log_coverage_misses` (on by default) names the missing column in the server log → `dbblue_columnar_add(...)` it + repopulate.
- **Correctness is automatic:** the engine serves a block only if it is all-visible and unchanged since build (visibility map + page LSN), else it reads the heap — columnar results never diverge from heap.
