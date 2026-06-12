# DBblue Incremental Materialized View Refresh

## Overview

DBblue extends PostgreSQL's `CREATE MATERIALIZED VIEW` with an `incremental_refresh` storage option. When enabled, the engine automatically installs AFTER STATEMENT triggers on every source table referenced by the view. Each trigger fires a C-level SPI function that applies a precomputed delta — updating only the rows in the matview that were affected by the DML — instead of recomputing the entire view.

No `REFRESH MATERIALIZED VIEW` is ever needed. The matview is always current by the time the triggering transaction commits.

```sql
-- Standard PostgreSQL (stale until manually refreshed):
CREATE MATERIALIZED VIEW mv_sales AS
    SELECT product_id, SUM(price_subtotal) AS revenue
    FROM sale_order_line GROUP BY product_id
WITH DATA;
-- ... later ...
REFRESH MATERIALIZED VIEW mv_sales;   -- must run this explicitly

-- DBblue (always current, zero maintenance):
CREATE MATERIALIZED VIEW mv_sales
    WITH (incremental_refresh=true) AS
    SELECT product_id, SUM(price_subtotal) AS revenue
    FROM sale_order_line GROUP BY product_id
WITH DATA;
-- matview updates itself on every INSERT/UPDATE/DELETE to sale_order_line
```

---

## Architecture

### Entry Points

| Function | File | Purpose |
|---|---|---|
| `MatviewIncrSetup` | `matview_incr.c` | Called after `CREATE MATERIALIZED VIEW … WITH DATA` completes. Analyses the view query, generates all delta SQL, writes catalog rows, installs triggers. |
| `MatviewIncrTeardown` | `matview_incr.c` | Called by `DROP MATERIALIZED VIEW`. Removes catalog rows and drops triggers. |
| `MatviewIncrNormalize` | `matview_incr.c` | Called before `MatviewIncrSetup`. Rewrites CTEs and FROM-subqueries into inline form so the query is in a canonical shape the delta builder can handle. |
| `matview_delta_apply` | `matview_incr.c` | The trigger function. Runs on every INSERT/UPDATE/DELETE to a tracked source table. Executes the precomputed delta SQL via SPI. |

### Catalog: `pg_dbblue_matview`

One row per `(matview OID, source table OID)` pair.

| Column | Type | Description |
|---|---|---|
| `mvrelid` | `oid` | Matview relation OID |
| `srctable` | `oid` | Source table OID |
| `ins_sql` | `text` | INSERT delta SQL — runs on source INSERT |
| `del_sql` | `text` | DELETE delta SQL — runs on source DELETE/UPDATE |
| `cln_sql` | `text` | Cleanup SQL — removes rows where `__mv_count__ ≤ 0` |
| `having_sql` | `text` | HAVING re-evaluation SQL (NULL if no HAVING clause) |
| `lock_sql` | `text` | Advisory-lock pre-step for MIN/MAX views (NULL otherwise) |

### Hidden Counter Column

Every incremental matview gets an extra `__mv_count__ bigint` column injected before `CREATE MATERIALIZED VIEW` executes. This column tracks how many source rows contribute to each group. It is used by `cln_sql` to garbage-collect groups whose last contributing row was deleted.

---

## Trigger Mechanism

Four AFTER STATEMENT triggers are installed on each source table:

| Trigger event | Transition table | Action |
|---|---|---|
| `AFTER INSERT` | `__mv_newtable__` | Run `ins_sql` against new rows |
| `AFTER DELETE` | `__mv_oldtable__` | Run `lock_sql` (if present), then `del_sql`, then `cln_sql` |
| `AFTER UPDATE` | both | Run `lock_sql` (if present), then `del_sql` + `cln_sql`, then `ins_sql` |
| `AFTER TRUNCATE` | none | Full recompute — see below |

AFTER STATEMENT triggers see the full set of changed rows as a transition table, so bulk operations (e.g. `UPDATE … WHERE product_id = 5`) are handled in a single SPI call regardless of how many rows changed.

### TRUNCATE handling

`TRUNCATE` removes every row from a source table at once and exposes no per-row transition data, so the incremental delta machinery cannot represent it. The `AFTER TRUNCATE` trigger therefore falls back to a full recompute: `matview_delta_apply` issues `REFRESH MATERIALIZED VIEW` on the matview. Because the stored view query already carries the hidden `__mv_count__` target, the refresh rebuilds every column — visible and hidden — from the current source-table contents. Without this trigger a `TRUNCATE` on a source table would silently leave the matview stale.

### Unpopulated-matview guard

`matview_delta_apply` skips all maintenance while the matview is unpopulated (`pg_class.relispopulated = false`). An unpopulated incremental matview has no baseline to apply deltas against — this happens, for example, during `pg_dump`/restore when the delta triggers are installed before the source data is loaded and before the matview is populated by the dump's `REFRESH`. Any DML in that window is captured wholesale by the eventual `REFRESH`.

---

## Delta SQL Patterns

### INSERT path (`ins_sql`)

Groups changed rows from `__mv_newtable__`, computes the partial aggregate, then UPSERTs into the matview:

```sql
WITH delta AS (
    SELECT product_id,
           SUM(price_subtotal) AS d_revenue,
           COUNT(*)            AS d_count
    FROM __mv_newtable__
    GROUP BY product_id
)
INSERT INTO mv_sales (product_id, revenue, __mv_count__)
SELECT product_id, d_revenue, d_count FROM delta
ON CONFLICT (product_id) DO UPDATE
    SET revenue      = mv_sales.revenue      + EXCLUDED.revenue,
        __mv_count__ = mv_sales.__mv_count__ + EXCLUDED.__mv_count__;
```

### DELETE path (`del_sql`)

Subtracts the delta of deleted rows:

```sql
WITH delta AS (
    SELECT product_id,
           SUM(price_subtotal) AS d_revenue,
           COUNT(*)            AS d_count
    FROM __mv_oldtable__
    GROUP BY product_id
)
UPDATE mv_sales mv
SET revenue      = mv.revenue      - d.d_revenue,
    __mv_count__ = mv.__mv_count__ - d.d_count
FROM delta d
WHERE mv.product_id = d.product_id;
```

### Cleanup (`cln_sql`)

Removes groups that no longer have any contributing rows:

```sql
DELETE FROM mv_sales WHERE __mv_count__ <= 0;
```

### HAVING re-evaluation (`having_sql`)

When the view has a HAVING clause, a fourth step re-checks the condition after each delta and removes groups that no longer satisfy it, or re-inserts groups that now do:

```sql
DELETE FROM mv_sales WHERE __mv_count__ > 0 AND NOT (revenue > 1000);
```

---

## MIN/MAX Aggregate Support

MIN and MAX require special handling because the delta is non-monotonic: deleting the current minimum requires a full-group rescan to find the new minimum.

### Two-Phase DELETE (advisory lock fix)

In READ COMMITTED mode, PostgreSQL takes a snapshot at the start of each SQL command. A single WITH chain that both acquires advisory locks and rescans the aggregate uses the pre-lock snapshot, which can miss concurrent INSERTs that committed between the start of the command and the lock acquisition.

The fix splits DELETE execution into two separate SPI calls:

**Phase 1 — `lock_sql`** (separate `SPI_execute_plan` call):
```sql
SELECT pg_advisory_xact_lock(mvrelid::int4, hashtext(product_id::text))
FROM (SELECT DISTINCT product_id FROM __mv_oldtable__) _aff_;
```
This gets a fresh snapshot, forcing all concurrent INSERTs to commit first, then acquires exclusive per-group advisory locks.

**Phase 2 — `del_sql`** (second `SPI_execute_plan` call):
```sql
WITH affected AS (
    SELECT DISTINCT product_id FROM __mv_oldtable__
),
old_delta AS (
    SELECT product_id, MIN(price_subtotal) AS d_min, MAX(price_subtotal) AS d_max
    FROM __mv_oldtable__ GROUP BY product_id
),
new_agg AS (
    SELECT product_id, MIN(price_subtotal) AS n_min, MAX(price_subtotal) AS n_max
    FROM sale_order_line
    WHERE product_id IN (SELECT product_id FROM affected)
    GROUP BY product_id
)
UPDATE mv_minmax mv
SET min_price = COALESCE(n.n_min, mv.min_price),
    max_price = COALESCE(n.n_max, mv.max_price)
FROM affected a LEFT JOIN new_agg n USING (product_id)
WHERE mv.product_id = a.product_id;
```
This second call gets another fresh snapshot — taken after the advisory locks are held — so it sees all concurrent INSERTs that committed before the locks were acquired in Phase 1.

---

## Supported View Shapes

| Feature | Phase | Notes |
|---|---|---|
| Single-table GROUP BY with COUNT/SUM | Phase 1 | Core foundation |
| Multi-table INNER JOIN + GROUP BY | Phase 2 / Phase 7 | N-table, equi-join |
| AVG aggregate | Phase 3 | Maintained as SUM/COUNT pair |
| HAVING clause | Phase 4 | Hidden-group architecture |
| WHERE clause | Phase 5 | Column comparisons, constants, boolean ops, IN lists, varchar equality (`RelabelType`) |
| Aggregate argument expressions | Phase 6 | `SUM(qty * price)` etc. |
| Row-level matview (no GROUP BY) | Phase 9 | Warns if source table has no PK in SELECT |
| FULL OUTER JOIN | Phase 10 | Handled as two separate delta paths |
| CROSS JOIN, non-equi joins | Phase 11 | Eligibility guards applied |
| DISTINCT | Phase 12 | Duplicate-suppression via count tracking |
| UNION ALL | Phase 13 | Each branch tracked independently |
| Self-join + GROUP BY | Phase 14 | Varno disambiguation |
| MIN / MAX aggregates | Phase 15 | Full-group rescan on DELETE |
| CTE / FROM-subquery normalization | Phase 16 | Inlined before delta generation |

---

## WHERE Clause Support

The WHERE clause validator (`incr_validate_expr`) and deparser (`incr_deparse_where_qual`) support:

- `Var` — column references
- `Const` — literal constants
- `OpExpr` — comparison and arithmetic operators (`=`, `<`, `+`, etc.)
- `BoolExpr` — `AND`, `OR`, `NOT`
- `NullTest` — `IS NULL`, `IS NOT NULL`
- `FuncExpr` — non-volatile, non-set-returning functions
- `ScalarArrayOpExpr` — `col IN (...)` / `col = ANY(...)`
- `ArrayExpr` — `ARRAY[...]` literals
- `RelabelType` — no-op type coercions (e.g. `varchar = 'literal'`)

Volatile functions and subqueries in WHERE are rejected at `CREATE MATERIALIZED VIEW` time with a descriptive error.

---

## Adversarial Test Coverage

All scenarios pass 3 rounds of 45-second concurrent-write stress testing:

| Scenario | Tests |
|---|---|
| B2 | SUM/COUNT delta path under concurrent INSERTs and DELETEs |
| B3 | MIN + MAX + SUM combined in one view |
| B4 | HAVING clause boundary — groups crossing the threshold |
| B5 | Multi-column GROUP BY with MIN/MAX |
| B6 | Last-row ghost prevention — group removed when final row deleted |
| B7 | JOIN + MIN/MAX across two tables |
| B8 | HAVING + MIN/MAX combined |
| B9 | UPDATE storms — INSERT and DELETE paths firing simultaneously |

Performance (B10): MIN/MAX DELETE throughput is ~50% of SUM/COUNT DELETE due to the full-group rescan. INSERT throughput is within 10% of SUM/COUNT.

---

## Production Verification

Tested against a real Odoo 17 database (266,000+ `sale_order_line` rows, 3,000+ `account_move` rows):

| Matview | Rows | Aggregates | Result |
|---|---|---|---|
| `mv_sales_by_product` | 588 groups | SUM, COUNT over `sale_order_line` | INSERT/UPDATE/DELETE all PASS |
| `mv_invoice_minmax` | 2,075 groups | MIN, MAX, COUNT over `account_move WHERE move_type='out_invoice'` | INSERT/DELETE all PASS |

Live Odoo sale order creation updated `mv_sales_by_product` instantly on `sale_order_line` INSERT, without any `REFRESH` call.

---

## Implementation Files

| File | Role |
|---|---|
| `src/backend/commands/matview_incr.c` | All delta generation, trigger execution, catalog I/O (~7,000 lines) |
| `src/include/commands/matview_incr.h` | Public entry points: `MatviewIncrSetup`, `MatviewIncrTeardown`, `MatviewIncrNormalize`, `MatviewIncrAddCountTarget` |
| `src/include/catalog/pg_dbblue_matview.h` | Catalog table definition (7 columns) |
| `src/include/catalog/pg_dbblue_matview_d.h` | Generated attribute number constants (`Anum_pg_dbblue_matview_*`) |
| `share/postgres.bki` | Bootstrap catalog — drives `initdb` schema creation |
| `src/backend/commands/createas.c` | Wires `MatviewIncrAddCountTarget` and `MatviewIncrSetup` into `ExecCreateTableAs` |
| `src/backend/commands/matview.c` | Wires `MatviewIncrTeardown` into `ExecDropTable` |

---

## Usage

```sql
-- Basic aggregation
CREATE MATERIALIZED VIEW mv_sales
    WITH (incremental_refresh=true) AS
    SELECT product_id,
           SUM(price_subtotal) AS revenue,
           COUNT(*)            AS line_count
    FROM sale_order_line
    GROUP BY product_id
WITH DATA;

-- With WHERE clause (including varchar comparisons)
CREATE MATERIALIZED VIEW mv_invoices
    WITH (incremental_refresh=true) AS
    SELECT partner_id,
           MIN(amount_untaxed) AS min_invoice,
           MAX(amount_untaxed) AS max_invoice,
           COUNT(*)            AS invoice_count
    FROM account_move
    WHERE move_type = 'out_invoice'
    GROUP BY partner_id
WITH DATA;

-- With HAVING
CREATE MATERIALIZED VIEW mv_top_customers
    WITH (incremental_refresh=true) AS
    SELECT partner_id,
           SUM(amount_total) AS total_spent
    FROM account_move
    WHERE move_type = 'out_invoice'
    GROUP BY partner_id
    HAVING SUM(amount_total) > 10000
WITH DATA;

-- JOIN across tables
CREATE MATERIALIZED VIEW mv_sales_by_category
    WITH (incremental_refresh=true) AS
    SELECT pt.categ_id,
           SUM(sol.price_subtotal) AS revenue,
           COUNT(*)                AS line_count
    FROM sale_order_line sol
    JOIN product_product pp ON pp.id = sol.product_id
    GROUP BY pt.categ_id
WITH DATA;

-- Drop removes triggers and catalog rows automatically
DROP MATERIALIZED VIEW mv_sales;
```

---

## Backup / Restore (`pg_dump`)

Incremental matviews survive a `pg_dump`/restore cycle with incremental maintenance fully re-armed. The mechanics:

- The `incremental_refresh` reloption is a standard `StdRdOptions` entry, so it is dumped as `WITH (incremental_refresh='true')` on the matview.
- The delta triggers are created internal (`tgisinternal`) and the engine-managed unique index carries an `INTERNAL` dependency on the matview, so **neither is dumped as a standalone object** — both are recreated by the engine on restore. (pg_dump's `getIndexes` skips indexes with an internal dependency on a relation.)
- pg_dump emits the matview as `CREATE MATERIALIZED VIEW … WITH NO DATA` followed by a standalone `REFRESH`. The `WITH NO DATA` create path re-installs the triggers, index, and catalog rows on the freshly defined (empty) matview; the subsequent `REFRESH` populates it.

### One-time backfills on restore (`MatviewIncrPostRefresh`)

Some shapes need a one-time backfill that can only run once the matview is populated:

- **HAVING** keeps groups that *fail* the condition (with `__mv_having_ok__ = false`) so they can be resurrected by a later delta. A full `REFRESH` only repopulates the *passing* groups, so the failing groups must be re-seeded from the source afterward.
- **UNION ALL** stores one row per distinct value with a `__mv_count__`; a full `REFRESH` reproduces the raw per-branch rows, which must then be collapsed.

`MatviewIncrPostRefresh()` runs these backfills at the end of every non-create `REFRESH`. Because the dump restores a matview as `WITH NO DATA` + a standalone `REFRESH`, this is exactly what re-arms HAVING/UNION matviews across a dump/restore — and it also makes a plain manual `REFRESH` of those matviews correct.

- For **HAVING**, the base matview (`_dbblue_<oid>_base`) and its filtering view restore as their own objects; the rename/view scaffolding is skipped (the base is already named) and only the delta wiring + failing-group seed are rebuilt.
- For **UNION ALL**, the unique index is also deferred: the restore `REFRESH` reloads the raw per-branch rows (which contain cross-branch duplicates), so the index is built by `MatviewIncrPostRefresh()` *after* the dedup collapses them — never while duplicates are present.

**Verified by** `src/test/dbblue_ivm/dump_restore_consistency.sh`: SUM/COUNT, AVG, MIN/MAX, JOIN, HAVING, and UNION ALL matviews dump, restore with a clean exit, and remain incrementally correct afterward — including a HAVING group that crosses the threshold after restore (correct only if the failing-group seed was rebuilt), UNION ALL dedup counts, and TRUNCATE re-seeding.

> **Note — fresh `WITH NO DATA` + HAVING.** A HAVING incremental matview must be created `WITH DATA` (its filtering view cannot be set up over an unpopulated base). Creating one `WITH NO DATA` emits a `WARNING` and leaves a plain matview. The dump/restore path is unaffected because the base is recognised by its name.

---

## Limitations

- **HAVING requires `WITH DATA`**: a HAVING incremental matview cannot be set up `WITH NO DATA` (a warning is emitted); dump/restore is unaffected.
- **No subqueries in WHERE**: correlated or uncorrelated subqueries in the WHERE clause are rejected. Use a JOIN instead.
- **No volatile functions in WHERE or aggregate arguments**: `now()`, `random()`, etc. are rejected because they would produce different results per-row vs. per-delta.
- **Outer joins with NULLs**: LEFT/RIGHT/FULL OUTER JOIN matviews are supported but the delta for the NULL-extended side requires full-group rescans, similar to MIN/MAX.
- **`__mv_count__` column**: always present in the matview. Queries against the matview should ignore it or exclude it from `SELECT *` projections.
- **No DDL on source tables while active**: `ALTER TABLE … ADD/DROP COLUMN` on a tracked source table requires dropping and recreating the matview.
