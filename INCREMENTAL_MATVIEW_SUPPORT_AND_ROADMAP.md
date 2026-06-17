# DBblue Incremental Materialized Views — Supported Queries & Roadmap

Companion to `INCREMENTAL_MATVIEW.md` (which documents the architecture). This
document answers three questions:

1. **What query shapes are supported today** (and at what confidence)?
2. **What is Phase 2** (the delta-core redesign)?
3. **What is planned beyond that** (the future roadmap)?

Every "supported / rejected" claim below reflects behavior verified by the test
suite in `src/test/dbblue_ivm/` and the adversarial concurrency battery.

---

## 1. Confidence tiers

| Tier | Meaning |
|---|---|
| ✅ **Proven** | Tested for correctness *and* concurrency (adversarial battery) and/or in production (Odoo, 266K rows); dump/restore verified. Rely on it. |
| 🟡 **Works, uncertified** | Single-threaded correctness verified, but **not** covered by the concurrency battery. Emits a `WARNING` at `CREATE`. Validate under your workload before production reliance. |
| ⛔ **Rejected (loud)** | Refused at `CREATE` with a clear, actionable error. Never silently wrong. |
| 🔒 **Guarded** | Allowed, but a runtime guard raises a clear error if an unsupported *data* condition occurs (e.g. a NULL group key). No silent corruption. |

---

## 2. Supported query shapes

### ✅ Proven (production-ready)

| Shape | Notes |
|---|---|
| `GROUP BY` on plain column(s), single table | core delta path |
| `SUM`, `COUNT(*)`, `COUNT(col)`, `AVG` | numeric / integer; AVG kept as a (sum,count) pair |
| `MIN`, `MAX` | two-phase advisory-lock rescan on delete |
| Multi-table **INNER JOIN** + `GROUP BY` | N-table, equi-join |
| `WHERE` | column comparisons, `AND`/`OR`/`NOT`, `IN (...)`, `IS NULL`, non-volatile functions, varchar/`RelabelType` |
| `HAVING` | hidden base matview + filtering view |
| `DISTINCT` (full) | converted to GROUP BY on all output columns |
| Aggregate **arithmetic** arguments | e.g. `SUM(qty * price)` |
| Aggregate **column aliasing** | e.g. `SELECT g AS k, MIN(amt) ...` (fixed this audit) |
| Filter/projection **CTEs** and **FROM-subqueries** | the normalizer **inlines** them at `CREATE` → the stored object is the plain form above; maintenance + dump/restore verified |
| Row-level matview (no `GROUP BY`) | warns if the source has no PK in SELECT |
| Lifecycle | `TRUNCATE`, `pg_dump`/restore, `DROP`, source-table DDL guards |

**Isolation:** verified consistent under READ COMMITTED, REPEATABLE READ, and
SERIALIZABLE (deltas are transactional with the source DML, so serialization
failures roll back both — no partial state).

### 🟡 Works, but not concurrency-certified (warns at `CREATE`)

| Shape | Status |
|---|---|
| `LEFT` / `RIGHT` / `FULL OUTER JOIN` | single-threaded correct; **not** in the concurrency battery |
| Self-join | single-threaded correct; not in the battery |
| `UNION ALL` | single-threaded correct; not in the battery (was crash-fixed this audit) |

A `WARNING` fires at `CREATE` for these: *"…not yet certified under concurrent
writes."* They are **not** blocked.

---

## 3. Rejected shapes (loud, with the fix)

| Shape | Why rejected | What to do instead |
|---|---|---|
| `COUNT(DISTINCT x)` | per-row delta can't track last-occurrence of a value | needs auxiliary state (roadmap) |
| `agg(...) FILTER (WHERE …)` | filter not yet honored by the delta | rewrite as `agg(CASE WHEN … )` once Phase 2 lands |
| `GROUP BY <expression>` (`date_trunc`, …) | delta keys on plain columns | add a generated/stored bucket column and group on it |
| `SUM`/`AVG` over `real`/`double precision` | float addition is non-associative → running total drifts | use `numeric` (exact) |
| Subquery in `WHERE` (`IN`/`EXISTS`) | sublink not maintainable per-table | rewrite as a `JOIN` |
| Scalar subquery in `SELECT` | sublink | rewrite as a `LEFT JOIN` + aggregate |
| Correlated subquery | sublink | rewrite as a `JOIN` |
| **Nested aggregation** (subquery that aggregates) | inherently two-level | use a regular matview + scheduled `REFRESH` |
| **Recursive CTE** | inherently recursive | regular matview + scheduled `REFRESH` |
| **Window functions** (`OVER(…)`) | not incrementally maintainable | regular matview + scheduled `REFRESH` |
| `LIMIT` / `OFFSET` | result set shifts on any change | — |
| Volatile functions (`now()`, `random()`) in WHERE / agg args | per-row vs per-delta divergence | — |
| `UNION`/`INTERSECT`/`EXCEPT` (non-`ALL`) | dedup semantics | use `UNION ALL` if applicable |

> **Note on CTEs/subqueries:** the rule is *"can it be flattened?"* A CTE or
> FROM-subquery that is only a filter/projection is **inlined and supported**; a
> subquery that **aggregates**, sits in **WHERE/SELECT**, is **correlated**, or
> is **recursive** is **rejected**. Per a survey of a live Odoo database,
> recursive CTEs (0 of 22 views) and nested aggregation (~2 of 18) are rare in
> the report layer; window functions (~4) are the more common non-maintainable
> pattern.

---

## 4. Data requirements & boundaries

- 🔒 **Group-key columns must not contain NULL values.** A runtime guard raises
  a clear error if an insert delta would introduce a NULL group key — never
  silent corruption. `NOT NULL`-schema keys pay nothing.
  **Consequence:** the guard fires inside the source `INSERT`'s transaction, so
  it **fails that source write** — while the matview exists, the grouped column
  effectively behaves like `NOT NULL` for the source table. Do not group an
  incremental matview on a column the application writes NULL to.
  **Escape hatch (verified):** add `WHERE <col> IS NOT NULL` to the matview —
  the source can then freely store NULLs in that column; the matview simply
  ignores those rows and maintains the non-NULL groups correctly.
- **HAVING matviews must be created `WITH DATA`** (a `WARNING` is emitted
  otherwise); dump/restore is unaffected.
- **Logical-replication subscribers do not maintain** the matview (delta
  triggers fire on origin only). Physical replicas are fine (data ships via WAL).
- **Residual:** a group whose values are *all* NULL shows `0` rather than
  SQL-exact `NULL` for `SUM` until the group empties. Bounded; documented.

---

## 5. Phase 2 — the delta-core redesign

### Why
The delta SQL is currently built by ~18 per-shape **string builders** with ~80
hand-written deparse sites. Every construct (a column, a NULL, an alias, a
`CASE`) must be hand-rendered at each site — which is why expressions/`CASE`/
`FILTER` aren't supported and why correctness bugs cluster in the complex shapes.
Cost to extend ≈ *O(shapes × constructs)*.

### What
Stop generating SQL by hand. Build the delta by **transforming the parsed
`Query` tree** and letting PostgreSQL render it:

1. Copy the matview's stored `Query`.
2. Repoint the changed table's range-table entry to its transition table
   (`__mv_newtable` / `__mv_oldtable`).
3. Render with `ruleutils` (the engine behind `pg_get_viewdef`) — so `CASE`,
   `COALESCE`, `FILTER`, expressions, types, and NULL semantics are handled by
   core, **for free**.
4. Merge via a small, fixed **apply** step driven by a **per-aggregate strategy**
   (SUM→add, COUNT→add, AVG→add (sum,count), MIN/MAX→rescan), with the
   **counting algorithm** (`__mv_count__` + per-SUM non-null counters) governing
   group lifecycle and exact NULL/empty semantics.

### Module structure (split the 7.6 k-line file)
`analyze` (eligibility/normalize) · `agg` (per-aggregate strategies) · `delta`
(build the delta Query) · `apply` (merge + MIN/MAX rescan) · `catalog` ·
`trigger` (plan cache) · `setup` (create/teardown/dump-restore).

### Migration — incremental, never big-bang (strangler pattern)
Build the new engine **beside** the old one behind the existing catalog/trigger
scaffolding. Migrate one shape at a time, each gated by the existing suite +
its concurrency-battery scenario, deleting the old builder as each lands:

> plain aggregate → INNER JOIN → MIN/MAX → outer / self / UNION ALL

As each complex shape moves onto the `Query`-tree path, its correctness stops
depending on hand-written SQL — which is exactly how the 🟡 shapes above become
✅ (this *is* the "certify under concurrency" work, done on durable code).

### What Phase 2 keeps (do **not** redesign)
The `pg_dbblue_matview` catalog, AFTER-STATEMENT/ENR trigger mechanism + plan
cache, `__mv_count__` counting, AVG-as-(sum,count), the two-phase MIN/MAX lock
*concept*, and all lifecycle work (dump/restore, TRUNCATE, teardown, DDL guards,
float rejection, NULL-key guard). These are sound; the new delta core plugs in.

### What Phase 2 unlocks (nearly free once the `Query`-tree path exists)
`SUM(CASE WHEN … )`, `COALESCE`, arbitrary scalar expressions in SELECT/WHERE,
`GROUP BY <expression>`, and `agg(...) FILTER (WHERE …)` — i.e. the shape of
essentially all `CASE`-heavy Odoo report views.

---

## 6. Future roadmap (beyond Phase 2)

Prioritized by value for Odoo reporting:

1. **`SUM(CASE WHEN …)` / `COALESCE` / scalar expressions** — the #1 Odoo report
   pattern; delivered as Phase 2's payoff.
2. **`GROUP BY <expression>`** (e.g. `date_trunc('month', d)`) — time-bucketed
   reports; the key becomes the expression.
3. **`agg(...) FILTER (WHERE …)`** — once expressions render via `ruleutils`.
4. **Certify outer / self / UNION ALL under concurrency** — via the battery on
   the `Query`-tree implementation (lifts them from 🟡 to ✅).
5. **`COUNT(DISTINCT)`** — needs a per-(group, value) auxiliary count table.
6. **Full NULL group-key support** — `NULLS NOT DISTINCT` index +
   `IS NOT DISTINCT FROM` joins (removes the 🔒 guard's restriction).
7. **Exact all-NULL `SUM` semantics** — per-column non-null counter.
8. *(Optional)* **automatic subquery→join rewrite** for WHERE/SELECT sublinks.

---

## 7. Out of scope (not planned — inherently non-incremental)

Window functions; recursive CTEs; nested/multi-level aggregation; correlated
subquery maintenance; percentile / ordered-set aggregates; `LIMIT`/`OFFSET`;
volatile functions. For these, use a **regular materialized view with a
scheduled `REFRESH`** — the engine rejects them loudly so the choice is explicit.

---

## 8. Test coverage (where the confidence comes from)

`src/test/dbblue_ivm/`:
`truncate_consistency.sql` · `dump_restore_consistency.sh` · `having_teardown.sql`
· `float_aggregate_rejection.sql` · `unsupported_aggregates.sql` ·
`null_and_alias_correctness.sql` · `null_key_guard.sql` · `ddl_on_source.sql` ·
`isolation_levels.sh` · `truncate_concurrency.sh` — plus the B2–B11 adversarial
concurrency battery (SUM/COUNT, MIN/MAX, HAVING, multi-key, JOIN+MIN/MAX,
UPDATE-storm, TRUNCATE-under-load).
