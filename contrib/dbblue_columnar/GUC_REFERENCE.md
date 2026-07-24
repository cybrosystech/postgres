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
| `dbblue_columnar.enabled` | bool | `off` | postgresql.conf / `ALTER SYSTEM` → **restart** | Master switch. Off = engine inert (no store, no paths). |
| `dbblue_columnar.memory_mb` | int (MB) | `128` (min 128) | postgresql.conf / `ALTER SYSTEM` → **restart** | Memory budget for the column store (DSA). Blocks stop building once hit. |
| `dbblue_columnar.autorefresh_database` | string | `''` (off) | postgresql.conf / `ALTER SYSTEM` → **restart** | The one database whose registered tables the background worker auto-**populates** and maintains (see "Auto-populate" below). Empty = worker idle → you must `dbblue_columnar_populate` manually. |
| `dbblue_columnar.enable_columnar_scan` | bool | `on` | `SET` / `ALTER DATABASE` / `ALTER ROLE` → **new connections** | Lets the planner read the store. Off = engine stays loaded + store kept, but queries use heap. **Use this to A/B columnar vs heap.** |
| `dbblue_columnar.log_coverage_misses` | bool | `on` | superuser `SET` / `ALTER SYSTEM` → reload | Emits a `LOG` naming the unregistered column(s) when a query on a registered table falls back to heap. Silence = full coverage. Dedup: once per table per backend. |
| `dbblue_columnar.auto_columnarize` | bool | `off` | postgresql.conf / `ALTER SYSTEM` → **reload** (`pg_reload_conf()`) | Let the engine auto-pick columns to columnarize (vs manual `_add`). |
| `dbblue_columnar.naptime` | int (s) | `60` (1–86400) | postgresql.conf / `ALTER SYSTEM` → **reload** | Seconds between auto-populate/refresh passes. |
| `dbblue_columnar.refresh_threshold` | int (%) | `20` (1–100) | postgresql.conf / `ALTER SYSTEM` → **reload** | How far a table's all-visible page count may drift from its build-time baseline before the worker rebuilds it. |

### "Takes effect" legend (PostgreSQL GUC context)
- **restart** (`PGC_POSTMASTER`): `enabled`, `memory_mb`, `autorefresh_database` — read once at server start.
- **reload** (`PGC_SIGHUP`): `auto_columnarize`, `naptime`, `refresh_threshold` — `pg_ctl reload` or `SELECT pg_reload_conf()`.
- **new connections** (`PGC_USERSET`): `enable_columnar_scan` — any user; `SET` in-session, or `ALTER DATABASE db SET ...` / `ALTER ROLE r SET ...` for persistence (existing/pooled connections keep the old value until they reconnect).
- **superuser set** (`PGC_SUSET`): `log_coverage_misses` — superuser `SET`, or `ALTER SYSTEM` + reload.

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

## Operational notes / gotchas
- **Ephemeral store.** DSA-backed, never WAL-logged, wiped on restart. Repopulate after every restart (or configure `autorefresh_database` so the worker rebuilds it).
- **A/B a query columnar vs heap:** `SET dbblue_columnar.enable_columnar_scan = off;` then run it, then `= on`. From an app with a connection pool (e.g. Odoo), use `ALTER DATABASE ... SET ...` and **restart the app** so its pooled connections reconnect.
- **Confirm routing** with `EXPLAIN`: look for `Custom Scan (DBBlueColumnarAgg)` (grouped-aggregate pushdown), `Custom Scan (DBBlueColumnarScan)` (row-serve, also the leaf of parallel plans), or `Parallel Custom Scan (DBBlueColumnarScan)`.
- **Coverage tuning:** if a report is slow, `dbblue_columnar.log_coverage_misses` (on by default) names the missing column in the server log → `dbblue_columnar_add(...)` it + repopulate.
- **Correctness is automatic:** the engine serves a block only if it is all-visible and unchanged since build (visibility map + page LSN), else it reads the heap — columnar results never diverge from heap.
