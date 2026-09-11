# DBblue — Feature Guide: How Each Feature Works & How to Use It

DBblue is a PostgreSQL 19 fork built for workloads that are **transactional and
analytical at the same time** — ERP systems like Odoo, where the same tables are
written and reported on constantly. It adds six features that make those workloads
faster **without ever changing a query's result.**

> **Two rules that never bend**
> 1. **Correctness first.** Every feature returns *exactly* what stock PostgreSQL
>    would — byte-identical — or it declines to act. No feature can produce a wrong answer.
> 2. **Off by default.** Nothing changes stock behavior until you explicitly turn it
>    on. You opt in per feature, per session, per matview, or per cluster.

All numbers below were measured on **`acme`** — a synthetic Odoo 19 Enterprise
database with **2,000,000 `account_move_line` rows** — and each is **byte-identical /
same-rows** verified.

---

## Quick reference

| Feature | Turn it on with | Default | Verified win (acme) |
|---|---|---|---|
| FK-join reduction | `SELECT dbblue_trust_foreign_keys();` | GUC on, but inert until FKs trusted | 4742 ms → 18.6 ms (**~255×**) |
| Incremental matviews (IVM) | `CREATE MATERIALIZED VIEW … WITH (incremental_refresh=true)` | off (per matview) | 1846 ms → 6.7 ms (**~276×**) |
| Columnar engine | `CREATE EXTENSION dbblue_columnar;` + register + populate | off | count/min-max **~1400–2750×**; report **~5×**; store **~15×** smaller |
| Count-cache | `SET dbblue_count_cache = on;` (+ cluster tracker) | off | 520 ms → 0.9 ms (**~562×**) |
| Offset-flip | `SET dbblue_offset_flip = on;` (builds on count-cache) | off | 4933 ms → 4.3 ms (**~1150×**) |
| Skip-unchanged REFRESH | `WITH (auto_skip_unchanged=true)` | off (per matview) | no-op REFRESH, heap not rewritten |

---

## 1. FK-join reduction

**What it does.** When a `LEFT JOIN` is on a **mandatory, trusted foreign key**, every
preserved-side row is guaranteed exactly one match — so the join can never null-extend.
DBblue reduces it to an `INNER` join (or removes it), which unlocks far better plans
even when the nullable side's columns are selected — the exact pattern behind slow Odoo
list views and reports.

**How it works.**
- A foreign key is used for planning **only after `dbblue_trust_foreign_keys()`
  verifies it** (RELY-style, per-constraint trust stored in a catalog).
- Trust **auto-withdraws** if an RI trigger is ever observed to be skipped — so it can
  never give a wrong answer.
- With the FK trusted, the planner rewrites `LEFT → INNER`; that lets it, e.g., drive
  from an index and stop at `LIMIT` instead of hash-joining millions of rows then sorting.

**How to use it.**
```sql
SELECT dbblue_trust_foreign_keys();          -- verify + trust every FK (run once)
-- dbblue_enable_fk_join_reduction is ON by default; nothing else needed.
```
To turn the effect off for testing: `SET dbblue_enable_fk_join_reduction = off;`

**Verified (acme).** An Odoo list-view shape — `LEFT JOIN account_move … ORDER BY
m.date DESC LIMIT 80` — went **4742 ms → 18.6 ms (~255×)**, identical 80 rows. The plan
flipped from a `Hash Left Join` over 2M×1M rows to an index-driven nested loop that stops
at the `LIMIT`.

**Good for clients.** ⭐ **The broadest, most transparent win.** Real Odoo instances are
full of big-table list views and reports that `LEFT JOIN` related tables and sort/filter
by them (a real client log showed **942 LEFT JOINs**). Fires automatically, no app changes.
**Limit:** only wins when the `LEFT→INNER` flip enables a better plan (e.g. an index
early-stop) — not literally every join.

---

## 2. Incremental Materialized Views (IVM)

**What it does.** A materialized view that **maintains itself on every base-table write**,
staying **byte-identical to a full `REFRESH`** — no refresh schedule, no staleness window.
Instead of re-running the whole query, an `AFTER STATEMENT` trigger applies just the delta.

**How it works.**
- Two strategies, chosen automatically: an **additive per-row delta** for
  `SUM`/`COUNT`/`AVG`, and an **affected-group recompute** for outer joins, self-joins,
  `DISTINCT`, `MIN`/`MAX`, and functionally-dependent columns.
- The invariant is enforced: incremental result == a full `REFRESH` at every isolation
  level (READ COMMITTED, REPEATABLE READ, SERIALIZABLE).
- Shapes that can't meet the guarantee are **rejected cleanly at `CREATE`** — never served stale.

**How to use it.**
```sql
CREATE MATERIALIZED VIEW sales_by_partner
  WITH (incremental_refresh=true) AS
  SELECT partner_id, count(*) AS n, sum(debit::numeric(16,2)) AS total
  FROM account_move_line WHERE partner_id IS NOT NULL GROUP BY partner_id;
-- writes to account_move_line now update the view automatically. No REFRESH needed.
```
> Tip: `sum` a **fixed-scale** numeric (`sum(x::numeric(16,2))`) to take the O(1)
> additive path. Unconstrained `numeric` sums use the recompute path to stay byte-identical.

**Verified (acme).** One base-table write maintained the view in **6.7 ms**, versus a
full `REFRESH` at **1846 ms** (**~276×**), byte-identical (0 differing rows). The win grows
with table size — the delta cost is independent of row count.

**Good for clients.** ⭐ **High value for heavy custom reports/dashboards** the client
currently refreshes on a schedule (stale) or recomputes live (slow): sales summaries,
inventory valuation, **consolidation**. **Limit:** requires building the matview (a bit
of dev) — it isn't visible in stock Odoo screens.

---

## 3. Columnar engine

**What it does.** A **column store** for chosen columns plus a `DBBlueColumnarScan` /
`DBBlueColumnarAgg` custom scan that accelerates analytic aggregates and integrates with
PostgreSQL's parallel aggregation — while returning **exactly the row store's result.**

**How it works.**
- Blocks are built only from **all-visible** heap pages, with **zone maps** (per-block
  min/max) and **dictionary compression** on low-cardinality columns.
- `count(*)` is answered from block row-counts; `min`/`max` from zone maps — without
  scanning rows. Range filters skip whole blocks via zone maps. Some aggregate rollups
  are pushed *into* the scan (`DBBlueColumnarAgg`).

**How to use it.**
```
# postgresql.conf (needs a PostgreSQL restart — one time):
shared_preload_libraries = 'dbblue_columnar'
dbblue_columnar.enabled = on
```
```sql
CREATE EXTENSION dbblue_columnar;                    -- per database
VACUUM (FREEZE, ANALYZE) account_move_line;          -- make pages all-visible
SELECT dbblue_columnar_add('account_move_line'::regclass,
                           ARRAY['account_id','balance','partner_id','date']);
SELECT dbblue_columnar_populate('account_move_line'::regclass);   -- builds the store
-- queries over those columns now use the columnar scan automatically.
```
> **After base-table writes, re-run `dbblue_columnar_populate(...)`** so the store fully
> covers the table (an auto-refresh daemon can do this: `dbblue_columnar.naptime`,
> `dbblue_columnar.refresh_threshold`).

**Verified (acme).**
- `count(*)` and `min/max/count`: **~1400–2750×** (answered from metadata/zone maps).
- Monthly report `date_trunc('month',date), sum(balance)`: **~5×** (aggregate pushdown).
- Storage for 4 analytic columns: **524 MB heap → 35 MB (~15×).**

**Good for clients.** ⭐ **For large clients** (10M–40M+ move lines) running analytic
ledger reports/pivots; the win grows with size. **Limits (be honest):** the `sum`-pushdown
win needs the store to cover ~100 % of rows — any un-populated write drops it to a tie
until you re-populate. On a small/cached DB there's no I/O to save, so numeric-sum reports
tie. It's a "big-data analytics accelerator," not a universal switch. (`count`/`min`/`max`
stay fast regardless of a small heap remainder.)

---

## 4. Count-cache

**What it does.** Caches the result of `count(*) FROM t WHERE <predicate>` — the exact
pattern behind every ERP list-view page count ("1–80 of N") — keyed by the predicate's
fingerprint and **invalidated by writes** via per-relation write stamps. The result is
always correct or recomputed.

**How it works.** A cached count is served **only if the relation's write stamp is
unchanged** since capture. Any write bumps the stamp → the entry is discarded → recompute.
The cache is **session-local**.

**How to use it — this one has a *two-level* enable.**
```
# postgresql.conf (needs a PostgreSQL restart — one time; provides the write stamps):
dbblue_track_relation_writes = on
```
```sql
SET dbblue_count_cache = on;    -- per session (or ALTER ROLE … SET … for a whole app)
SELECT count(*) FROM account_move_line WHERE state='posted';   -- 2nd identical call: cache hit
```
> If `dbblue_track_relation_writes` is off, `dbblue_count_cache = on` does nothing —
> there are no stamps to validate against, so it safely declines. The tracker adds a small
> (~≲1 %) always-on write-path cost, cluster-wide — that's why it's a restart-level opt-in.

**Verified (acme).** `count(*) … WHERE balance > 5000`: **520 ms cold → 0.9 ms cached
(~562×)**; a subsequent write correctly recomputed it.

**Good for clients.** Situational. Helps **repeated identical counts on relatively stable
tables**. **Limits:** many real Odoo counts include `EXISTS`/joins (multi-relation → not
cached today), and hot tables invalidate the entry constantly, so steady-state hit-rate can
be low. Don't headline it — offer it for pagination-heavy screens on stable data.

---

## 5. Offset-flip

**What it does.** Deep pagination — `ORDER BY … LIMIT k OFFSET n` where `n` is past the
midpoint — is rewritten to **count from the near end instead**, reusing the cached row count.
Same rows, a fraction of the work.

**How it works.** It reads the total row count from the **count-cache**, and if `OFFSET`
is past the midpoint, scans the index **backward** and stops early — returning the identical
page in reverse-then-corrected order.

**How to use it.** (Builds on the count-cache, so both must be on and the count primed.)
```sql
SET dbblue_count_cache = on;  SET dbblue_offset_flip = on;
SELECT count(*) FROM big WHERE id > 0;                          -- primes the total (same session, same predicate)
SELECT id FROM big WHERE id > 0 ORDER BY id LIMIT 5 OFFSET 999500;   -- now flips
```
> **Two things must match:** the primed count and the paginated query must use the **same
> `WHERE` predicate**, on the **same connection**. In Odoo this lines up naturally — a list
> view runs `count(*)` for its pager, then the `LIMIT/OFFSET` page fetch, same request.

**Verified (acme).** `… ORDER BY id LIMIT 5 OFFSET 1999000` on 2M rows: **4933 ms →
4.3 ms (~1150×)**, identical rows — plan flipped to `Index Only Scan Backward` reading
~1000 index entries instead of ~1,999,000.

**Good for clients.** Situational. Only helps **deep pagination**, which real users rarely
do (a real client log had **zero** deep pages). A great benchmark number; niche in practice.

---

## 6. Skip-unchanged REFRESH

**What it does.** A `REFRESH MATERIALIZED VIEW` becomes a **no-op when nothing the view
depends on has changed** — the heap isn't rewritten at all.

**How it works.** Uses the same write-stamp mechanism: if no source relation changed since
the last refresh, the rebuild is skipped and the existing data is kept.

**How to use it.**
```sql
CREATE MATERIALIZED VIEW report WITH (auto_skip_unchanged=true) AS <query>;
-- a REFRESH against unchanged sources now skips the heap rewrite.
```

**Verified (acme).** After a no-op `REFRESH`, the matview's `pg_relation_filenode` is
unchanged (rebuild skipped), where a stock matview always rewrites its heap.

**Good for clients.** Niche — for scheduled refresh jobs that mostly run against quiet
tables. Backend-only; not something a client sees in a screen.

---

## Which feature for which client — the honest tiering

| Tier | Feature | Really good for… | Show it in the Odoo UI? |
|---|---|---|---|
| **1 — lead with it** | FK-join reduction | Any client with slow big-table list views/reports | ✅ Sort a big list by a related column |
| **2 — high value, some setup** | IVM | Heavy scheduled/live report recomputes (consolidation, summaries) | ⚠️ Via a custom report/matview |
| **2 — for large clients** | Columnar | Millions of ledger rows, analytic reports/pivots, at scale | ⚠️ Specific reports, at scale |
| **3 — situational** | Count-cache | Repeated counts on stable tables | ❌ Hard to perceive |
| **3 — situational** | Offset-flip | Deep-pagination workloads (rare) | ❌ Users don't page deep |
| **3 — niche** | Skip-unchanged | Scheduled refresh jobs on quiet tables | ❌ Backend only |

**Pitch guidance:** lead with **FK-join reduction** (broad, transparent) and **IVM**
(dramatic on heavy reports); bring **columnar** for large clients. Present the Tier-3 three
as "also available for specific workloads," not headline claims — that keeps the pitch
credible.

---

## Reproduce the numbers

All six, before-vs-after on your data, in one command:
```bash
bash /path/to/show_all_features.sql   # or run each toggle in psql (SET … = off/on)
```
Every result is byte-identical / same-rows to stock PostgreSQL — that's the whole point:
**performance earned inside correctness, never traded for it.**
