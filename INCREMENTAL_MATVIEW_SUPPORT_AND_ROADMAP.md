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
| 🟡 **Proven, serialized** | Same correctness + concurrency guarantees as ✅ — consistent with a full `REFRESH` at every isolation level — but maintenance of this matview **serializes** under concurrent writes (it takes a matview-level lock; emits a `NOTICE` at `CREATE`). A throughput consideration, **not** a correctness caveat. |
| ⛔ **Rejected (loud)** | Refused at `CREATE` with a clear, actionable error. Never silently wrong. |
| 🔒 **Guarded** | Allowed, but a runtime guard raises a clear error if an unsupported *data* condition occurs (e.g. a NULL group key). No silent corruption. |

---

## 2. Supported query shapes

### ✅ Proven (production-ready)

| Shape | Notes |
|---|---|
| `GROUP BY` on plain column(s), single table | core delta path |
| `GROUP BY` on an **expression** | e.g. `date_trunc('month', d)`, `(amt % 10)`, `CASE …`; must be IMMUTABLE and appear in the SELECT list. Maintained by the deparse core; single-table or INNER JOIN, no MIN/MAX/self-join |
| `SUM`, `COUNT(*)`, `COUNT(col)`, `AVG` | numeric / integer; AVG kept as a (sum,count) pair |
| `MIN`, `MAX` | delete-rescan, serialized on the matview-level lock (see 🟡 below); N-table joins OK |
| Multi-table **INNER JOIN** + `GROUP BY` | N-table, equi-join (additive via deparse; MIN/MAX via the hand rescan — both correct for 3+ tables) |
| `WHERE` | column comparisons, `AND`/`OR`/`NOT`, `IN (...)`, `IS NULL`, non-volatile functions, varchar/`RelabelType` |
| `HAVING` | hidden base matview + filtering view |
| `DISTINCT` (full) | converted to GROUP BY on all output columns |
| Aggregate **arithmetic** arguments | e.g. `SUM(qty * price)` |
| Aggregate **column aliasing** | e.g. `SELECT g AS k, MIN(amt) ...` (fixed this audit) |
| Filter/projection **CTEs** and **FROM-subqueries** | the normalizer **inlines** them at `CREATE` → the stored object is the plain form above; maintenance + dump/restore verified |
| Row-level matview (no `GROUP BY`) | warns if the source has no PK in SELECT |
| Lifecycle | `TRUNCATE`, `pg_dump`/restore, `DROP`, source-table DDL guards |

**Isolation:** every supported shape is verified consistent with a full
`REFRESH` under READ COMMITTED, REPEATABLE READ, and SERIALIZABLE. Deltas are
transactional with the source DML, so a serialization failure rolls back both —
no partial state. The **additive** shapes above (single-table / INNER JOIN
`SUM`/`COUNT`/`AVG`) are lock-free and keep full per-group write concurrency at
all levels (their `ON CONFLICT` upserts serialize only on the matview *row*).

### 🟡 Proven, but maintenance serializes (NOTICE at `CREATE`)

These shapes recompute or overwrite a region per delta rather than accumulating
additively, so they take a **matview-level advisory lock** that serializes their
maintenance. That is exactly what makes them consistent with a full `REFRESH` at
**every** isolation level, READ COMMITTED included — concurrent writers to the
source tables simply apply their deltas one at a time. All are in the concurrency
battery (`concurrency_exotic.sh`, READ COMMITTED + REPEATABLE READ).

| Shape | Status |
|---|---|
| `LEFT` / `RIGHT` / `FULL OUTER JOIN` | correct at all isolation levels; serialized maintenance |
| Self-join | correct at all isolation levels; serialized maintenance |
| `UNION ALL` | correct at all isolation levels; serialized maintenance |
| `MIN` / `MAX`, row-level (no `GROUP BY`) | correct at all isolation levels; serialized maintenance |

A `NOTICE` (not a `WARNING`) fires at `CREATE` for these:
*"<shape>; its maintenance is serialized under concurrent writes."* They are
**not** blocked, and the result is never wrong — only serialized.

---

## 3. Rejected shapes (loud, with the fix)

| Shape | Why rejected | What to do instead |
|---|---|---|
| `COUNT(DISTINCT x)` | per-row delta can't track last-occurrence of a value | needs auxiliary state (roadmap) |
| `agg(...) FILTER (WHERE …)` | filter not yet honored by the delta | rewrite as `agg(CASE WHEN … )` once Phase 2 lands |
| `GROUP BY` a **volatile** expression (`random()`, `now()`) or a STABLE one (e.g. `date_trunc` over `timestamptz`) | the same row could map to different groups across its insert- vs delete-delta → drift | use an IMMUTABLE expression (e.g. `date_trunc` over `timestamp`) or a generated/stored bucket column |
| `GROUP BY <expression>` not in the SELECT list, or with `MIN`/`MAX` / self-join | no output column to key on / shape the deparse core does not build | add the expression to SELECT; drop MIN/MAX or the self-join |
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

- **NULL group keys — full fidelity (incl. MIN/MAX); only self-join excludes.**
  A NULL (or partial-NULL, multi-column) GROUP BY/DISTINCT key is **maintained
  with full fidelity** — the incremental matview keeps the NULL group exactly like
  a normal matview — for the additive shapes (single-table / INNER JOIN / DISTINCT
  / HAVING) **and for MIN/MAX**, via a `NULLS NOT DISTINCT` unique index plus
  `IS NOT DISTINCT FROM` delta/rescan key matching. Verified `== REFRESH` in
  `null_key_exclusion.sql`.
  **Residual (self-join only):** a self-join matview still **auto-excludes** NULL
  keys — its recompute path matches keys with `=`/`IN` — so at `CREATE` the engine
  injects `<key> IS NOT NULL` for every *nullable* key and emits a `NOTICE`. The
  source write always succeeds; the matview omits the NULL group but stays
  consistent with its own `REFRESH`. Bounded and announced at `CREATE`.
- **HAVING matviews must be created `WITH DATA`** (a `WARNING` is emitted
  otherwise). A HAVING matview is stored as a user-facing view over a hidden
  `_dbblue_<oid>_base` matview, so a **full-database** `pg_dump`/restore works
  (both objects ship — verified by `dump_restore_consistency.sh`). A **selective,
  object-filtered dump** (`pg_dump -t '<pattern>'`) matches the view's name but
  not the hidden base, leaving the restored view dangling — back HAVING matviews
  up with a full-database dump, not `pg_dump -t`. (Inherent to `-t`, which by
  design excludes objects that don't match the pattern.)
- **Logical-replication subscribers do not maintain** the matview (delta
  triggers fire on origin only). Physical replicas are fine (data ships via WAL).
- **All-NULL `SUM` returns SQL `NULL`** (not `0`) once a group loses its last
  non-NULL input, and recovers to a number when one returns — for **every** SUM
  shape: the additive ones (single-table / INNER JOIN / HAVING) and **MIN/MAX**,
  both via the hidden non-NULL counter `__mv_sumcnt_<col>`. The MIN/MAX path
  keeps `SUM` on **delta arithmetic** (so it still composes with the insert delta
  — a single statement that both deletes and inserts stays correct, guarded by
  `audit_regressions` BUGF) and uses the counter only to display `NULL`. Verified
  `== REFRESH` in `null_sum_fidelity.sql`. (Self-joins need no counter — their
  recompute path yields `NULL` directly.)

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
depending on hand-written SQL. (The "certify under concurrency" work is **done**:
the recompute/multiset shapes now serialize on a matview-level lock and are
consistent at every isolation level — they sit in the 🟡 tier above, correct but
serialized, no longer "uncertified".)

### What Phase 2 keeps (do **not** redesign)
The `pg_dbblue_matview` catalog, AFTER-STATEMENT/ENR trigger mechanism + plan
cache, `__mv_count__` counting, AVG-as-(sum,count), the matview-level
serialization lock for recompute/multiset shapes, and all lifecycle work
(dump/restore, TRUNCATE, teardown, DDL guards,
float rejection, NULL-key guard). These are sound; the new delta core plugs in.

### What Phase 2 unlocks (nearly free once the `Query`-tree path exists)
`SUM(CASE WHEN … )`, `COALESCE`, arbitrary scalar expressions in SELECT/WHERE,
`GROUP BY <expression>`, and `agg(...) FILTER (WHERE …)` — i.e. the shape of
essentially all `CASE`-heavy Odoo report views — **plus full NULL-group
fidelity** (`NULLS NOT DISTINCT` + `IS NOT DISTINCT FROM`), so an incremental
matview keeps the NULL group exactly like a normal matview instead of excluding
it.

---

## 6. Future roadmap (beyond Phase 2)

Prioritized by value for Odoo reporting:

1. **`SUM(CASE WHEN …)` / `COALESCE` / scalar expressions** — the #1 Odoo report
   pattern; delivered as Phase 2's payoff.
2. ✅ **Done — `GROUP BY <expression>`** (e.g. `date_trunc('month', d)`) —
   time-bucketed reports; the immutable, selected expression is the key,
   maintained by the deparse core (single-table / INNER JOIN, no MIN/MAX).
3. **`agg(...) FILTER (WHERE …)`** — once expressions render via `ruleutils`.
4. ✅ **Done — concurrency for outer / self / UNION ALL / MIN/MAX** — a
   matview-level serialization lock makes them consistent at every isolation
   level (READ COMMITTED included). Possible refinement: per-group locks so
   non-overlapping groups of one such matview maintain concurrently.
5. **`COUNT(DISTINCT)`** — needs a per-(group, value) auxiliary count table.
6. **Full NULL-group fidelity (match a normal matview)** — `NULLS NOT DISTINCT`
   index + `IS NOT DISTINCT FROM` predicates, so the NULL group is *kept and
   maintained* instead of auto-excluded. Delivered as part of Phase 2 (the
   `Query`-tree path makes the NULL handling uniform). Until then, incremental
   matviews exclude the NULL group (writes never blocked, self-consistent).
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
