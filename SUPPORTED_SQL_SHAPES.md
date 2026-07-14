# DBblue Incremental Materialized Views — Supported SQL Shapes

The contract for `CREATE MATERIALIZED VIEW … WITH (incremental_refresh=true)`.

**The one invariant, always:** an incremental matview is **byte-identical to a
full `REFRESH MATERIALIZED VIEW`** at every step. The engine never maintains a
view approximately. A view whose shape cannot be maintained is **rejected at
`CREATE` time** with a `feature_not_supported` (SQLSTATE `0A000`) error whose
message names the reason — it is never accepted and then maintained wrong.

**How to check any view (no data needed):** the gate runs at DDL time, so
```sql
CREATE MATERIALIZED VIEW _probe WITH (incremental_refresh=true) AS <select> WITH NO DATA;
```
either succeeds (supported) or raises `cannot use incremental_refresh: <reason>`.
`src/test/dbblue_ivm/corpus_classifier.sql` automates this over a whole schema
(`pg_get_viewdef` across `pg_views`) and prints a coverage % + rejection
histogram.

---

## Supported

### Join shapes
- **Single table.**
- **INNER joins**, any number of tables.
- **LEFT / RIGHT / FULL OUTER joins.** Optional (nullable) dimensions reached
  directly, **multi-hop** (fact→product→template), **off an inner-joined table**,
  or on **independent optional branches** are all maintained — including the
  orphan / de-orphan transitions (a fact row losing or gaining its match).
- **Two-way self joins** (a table joined to itself once). A table appearing
  **more than twice** (diamond patterns) is rejected.

### Aggregates (`GROUP BY`)
- **Additive**, maintained by per-row delta: `SUM`, `COUNT`, `COUNT(*)`, `AVG`,
  `MIN`, `MAX`.
- **Recompute-maintained** (each affected group re-derived from the live tables):
  `STDDEV`, `VARIANCE`, `BOOL_AND`, `BOOL_OR`, `STRING_AGG`, `ARRAY_AGG`,
  `JSON_AGG`, `JSONB_AGG`; **`COUNT(DISTINCT …)`**; **any aggregate with a
  `FILTER (WHERE …)` clause**; float `SUM`/`AVG` (recompute avoids rounding
  drift).
- Aggregate **arguments** may be expressions (`SUM(a*b)`, `SUM(CASE …)`), as long
  as they are immutable; complex/JSON argument expressions are supported over
  single-table and INNER-join shapes.

### GROUP BY keys
- Plain columns, and **expression / date-bucket keys**:
  `to_char(d,'YYYY-MM')`, `date_trunc('month', d)`, `x % n`, etc.
- **STABLE** key functions are allowed (not only IMMUTABLE) — see *Caveats*.
- Over an outer join, an expression key must reference only the **preserved
  anchor / inner-joined** tables (not an optional side).
- **Every GROUP BY key must also be a SELECT output column** — a key referenced
  only inside an expression (e.g. `GROUP BY a,b` while selecting only `a||b`) is
  rejected; add it to the SELECT list.

### SELECT-list projections
- Bare group keys and bare aggregates, and **immutable projections over them**:
  `COALESCE(SUM(a),0) + SUM(b)`, `SUM(x) / NULLIF(COUNT(*),0)`,
  `CASE WHEN SUM(a) > 0 THEN … END`, `a || b`, `jsonb_col ->> 'k'`, `NULL::t`,
  arithmetic. (These force the recompute path.)
- Non-immutable output expressions (`now()`, `CURRENT_DATE`, `concat()` — which
  is STABLE) are rejected; put them in a plain view layered on top.

### HAVING
- Comparisons / boolean combinations over aggregates, group keys, and constants —
  **including expressions over aggregates**: `HAVING COALESCE(SUM(a),0) > 0`,
  `HAVING CASE WHEN … END`, `HAVING SUM(a)+SUM(b) > k`.
- May reference a **stored projection output column** (composes with the
  projections above).
- A HAVING aggregate that is **not stored** (absent from the SELECT list) is
  rejected — nothing to evaluate the flag against.

### Other
- **Row-level views** (no `GROUP BY`): plain projections/filters, incl. joins.
  The view should select a **primary/unique key** so rows can be maintained (a
  warning is issued otherwise).
- **Full `DISTINCT`** (`SELECT DISTINCT a, b …`) — treated as `GROUP BY` on all
  output columns.
- **`WHERE`** filters: column refs, constants, comparisons, boolean ops, `IN`
  lists.
- **`UNION ALL`** of row-level branches.
- **`ORDER BY`** (ignored for maintenance, as in any matview).

---

## Not supported (rejected cleanly at CREATE)

| Shape | Reason string (verbatim) | Workaround |
|---|---|---|
| Scalar / correlated subqueries (`hasSubLinks`) | `subqueries are not supported` | inline, or a top view |
| `FROM`-clause derived tables | `only plain table references are supported (no functions, VALUES, etc.)` | flatten the join / **M4** |
| CTEs (`WITH …`) | `WITH clauses (CTEs) are not supported; inline the subquery instead` | inline / **M4** |
| Window functions (`… OVER …`) | `window functions cannot be maintained incrementally` | top view |
| `UNION`/`INTERSECT`/`EXCEPT` (dedup set ops) | `only UNION ALL is supported for set operations; UNION DISTINCT, INTERSECT, and EXCEPT are not supported` | `UNION ALL` if dedup not needed |
| `DISTINCT ON (…)` | `DISTINCT ON is not supported; use full DISTINCT (DISTINCT on all output columns)` | full `DISTINCT` |
| `LATERAL` joins | `LATERAL joins are not supported for incremental refresh` | flatten / top view |
| `LIMIT` / `OFFSET` | `LIMIT/OFFSET cannot be maintained incrementally; the result set would shift on every row change` | top view |
| Ordered-set aggregates (`percentile_cont … WITHIN GROUP`) | `aggregate "…" not supported (supported: …)` | — |
| Non-immutable output expr (`now()`, `CURRENT_DATE`) | `SELECT expression must be immutable — a deterministic function of the group's keys and aggregates; …` | top view |
| VOLATILE group key | `GROUP BY <volatile expr>` rejected | — |
| Expression group key over an optional (outer) side | rejected | — |
| GROUP BY key not in SELECT | `every GROUP BY key must also be a SELECT output column; …` | add the key to SELECT |
| `HAVING` without `GROUP BY` | `HAVING requires GROUP BY` | — |
| Unbound HAVING aggregate | `HAVING uses unsupported expressions; only maintained aggregates …` | select the aggregate |
| A table appearing 3+ times (diamond self-join) | `table "…" appears more than twice; diamond join patterns are not supported` | — |
| Niche FULL-join shapes (3-table FULL, FULL self-join) | rejected | — |

> **`ROLLUP` / `GROUPING SETS` / `CUBE`** are *accepted* by the gate and pass a
> basic differential, but are **not yet in the permanent regression suite** —
> treat as unverified and confirm in real-workload validation before relying on
> them.

---

## Operational model

- **Hidden columns** (reserved `__mv_` prefix; a user column using it is
  rejected): `__mv_count__` (per-group `COUNT(*)`, drives zero-count cleanup),
  `__mv_sumcnt_<col>` / `__mv_avgsum_<col>` / `__mv_avgcnt_<col>` (SUM/AVG NULL
  fidelity), `__mv_having_ok__` (HAVING visibility flag).
- **HAVING storage:** the physical matview is renamed `_dbblue_<oid>_base` and
  stores *all* groups (passing and failing); a plain **view under the original
  name** filters `WHERE __mv_having_ok__`. A per-delta step recomputes the flag.
- **Concurrency:** additive shapes keep per-group write concurrency. Recompute /
  outer-join / self-join / MIN-MAX shapes take a **matview-level advisory lock**
  (serialized maintenance under concurrent writers, for READ-COMMITTED-correct
  results) — a documented throughput characteristic.
- **Maintenance runs inside the writing transaction** (AFTER-STATEMENT triggers),
  so maintenance cost is part of the writer's commit latency.
- **TRUNCATE** on a source → full `REFRESH` fallback.

### Tuning GUCs
- **`dbblue_ivm_refresh_threshold`** (real, default `0.5`): the cost router. When
  a statement's affected rows exceed this fraction of the source table's
  estimated live tuples, the delta is applied by a full `REFRESH` instead of
  incrementally (bulk DML). `0` disables the fallback.
- **`dbblue_ivm_deparse_delta`** (bool, default `on`): build delta SQL via the
  query-tree deparse core; developer escape hatch.

---

## Correctness caveats

- **STABLE group key drift.** A STABLE key (e.g. `to_char` under a locale, or a
  timezone-dependent bucket) is re-derived correctly for *touched* groups, but an
  *untouched* group keeps its old value if `lc_time`/`TimeZone` later changes — a
  full `REFRESH` re-syncs. IMMUTABLE keys have no such caveat.
- **Query semantics are matched, not fixed.** If a view multiplies rows via a
  fan-out (e.g. independent one-to-many LEFT joins), incremental maintenance
  reproduces the *same* fan-out result a full `REFRESH` produces — it does not
  "correct" the query.
- **The cost router is a heuristic** (a ratio of estimated tuples); it changes
  *which path* maintains a delta, never the result.

---

## Verification

Every supported shape above is checked `== REFRESH` by the `src/test/dbblue_ivm/`
suite (differential vs an identically-defined plain matview, plus randomized
multi-thousand-step DML differentials). The classifier (`corpus_classifier.sql`)
is the tool to measure coverage on a real database. This document reflects the
engine as of the freeze; re-run the classifier after any engine change.
