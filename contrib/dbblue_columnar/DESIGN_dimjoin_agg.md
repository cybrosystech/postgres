# Design: In-node dimension hash-join for `DBBlueColumnarAgg`

**Status:** proposal / pre-implementation review
**Feature name (working):** aggregate-over-star-join pushdown ("dim-join agg")
**GUC (proposed):** `dbblue_columnar.enable_dimjoin_agg` — **default off**

---

## 1. Problem

Odoo analytic reports have the shape *"aggregate a fact table joined to one or more small
dimensions, grouping by a fact expression plus dimension columns."* Example (real, verbatim from
`stock_move`):

```sql
SELECT date_trunc('month', timezone('America/La_Paz', timezone('UTC', sm.date))::timestamp),
       sm.picking_type_id, count(*), sum(sm.quantity)
FROM stock_move sm
LEFT JOIN stock_picking_type spt ON spt.id = sm.picking_type_id
WHERE sm.state='done' AND sm.company_id=1
GROUP BY 1, sm.picking_type_id, spt.sequence, spt.id;
```

Today this plans as **plain PostgreSQL `Partial HashAggregate` *above* a `Hash Left Join`** — the join
materialises every matching fact row (~22.7M on the live 40M-row table) *before* aggregating. Two
independent reasons the columnar engine cannot help as-is:

1. **Top-level fused `DBBlueColumnarAgg` can't sit on a join** — its input must be the column store
   (it iterates blocks), not a join's output. It only fits *top-level over a scan* or *eager below a
   join*.
2. **Core PG's eager aggregation won't push a partial agg below a join when a grouping key is an
   *expression*** (proven: with a plain column key it does; `date_trunc(timezone(...))` it does not —
   and it is *not* a volatility issue: `timezone(text, timestamp)` is IMMUTABLE). So there is no
   `rel->grouped_rel` for the engine to fuse onto.

### Measured benefit (2M-row star-schema repro, warm cache)

| Plan | Shape | Execution |
|---|---|---|
| Current (expression key) | plain agg above join over 1,777,778 rows | **3,422 ms** |
| Fact aggregated in-engine (this feature's ceiling) | `DBBlueColumnarAgg`, join avoided | **965 ms** (~**3.5×**) |
| Generated column + eager | `DBBlueColumnarAgg` below join, join sees 4,240 rows | ≈ same as above |

Extrapolated to the live 40M-row query: **~50s → ~15s** (the columnar fact scan alone is already ~13s;
the join + above-join agg is the removable ~35s).

### Why not the cheaper alternatives

- **Generated column + eager aggregation** achieves the same speed **but is impractical for Odoo**: the
  timezone is baked into the column, yet Odoo emits the zone from each *user's* setting (a column per
  timezone + a 40M-row table rewrite each), and Odoo's SQL generator will not reference a custom column.
- **Expression statistics alone** (`CREATE STATISTICS ON (expr)`) fix only the **non-join** monthly
  reports (they correct the group-count estimate); the join query still won't eager an expression key.

**This feature is the only approach that accelerates the Odoo query *as generated* — inline expression
key + join, no schema change.**

---

## 2. Approach

Extend `DBBlueColumnarAgg` (grouped mode) to **aggregate the fact in-engine while probing a small
in-memory hash of the dimension** for dimension-sourced group keys — collapsing `Agg + Join + Scan`
into one node. The dimension is **read live per query under the query snapshot** (never cached in the
column store), so it inherits a `Hash Join`'s exact MVCC semantics and adds no staleness surface.

```
                 Agg  (GROUP BY fact-expr, fact.fk, dim.a, dim.b)
                  │
   ┌──────────────┴───────────────┐        ┌──────────────────────────────┐
   │  fact  LEFT JOIN  dim         │   ==>  │  DBBlueColumnarAgg (fused)     │
   │        ON fact.fk = dim.pk    │        │   • scan fact column store     │
   └──────────────────────────────┘        │   • probe dim hash by fact.fk  │
                                            │   • group by fact keys +       │
                                            │     looked-up dim attrs        │
                                            └──────────────────────────────┘
```

The per-row executor hook already exists — see [§6](#6-executor). The hard part is the **planner
pattern-match** ([§5](#5-planner-integration)).

---

## 3. Eligibility gate — prove-or-fall-back

**Philosophy:** if the engine cannot *prove* the shape is safe, it emits **nothing** and PostgreSQL's
normal plan runs. An unsupported query is never made incorrect — it is simply not accelerated.

Phase-1 gate (all must hold, else bail):

| # | Condition | Why |
|---|---|---|
| G1 | Exactly **one fact** relation, a base rel registered in the column store, heap AM, permanent | Reuses the existing scan-side gate; the fact is what we serve from blocks |
| G2 | Exactly **one dimension** relation (Phase 1) | Keep the join tree trivial; multi-dim is Phase 2 |
| G3 | Join is a single **equality** condition `fact.fk = dim.key`, INNER or LEFT (fact on the outer side) | Anything else changes row semantics |
| G4 | `dim.key` is **provably UNIQUE** (PK or unique constraint/index) | Guarantees many-to-one → **no fan-out** → a single hash entry per key is correct |
| G5 | Dimension is **small**: `dim` estimated rows ≤ `dimjoin_max_dim_rows` (proposed default 65536) | Bounds the in-memory hash; else fall back |
| G6 | **Dimension-side `WHERE` quals** — allowed for **INNER** joins (Phase 2): applied per dim row at build, so a fact row whose fk matches no *passing* dim row is dropped. Still rejected for a preserved LEFT join (the `WHERE d.x IS NULL` anti-join idiom that does not reduce — Phase 2b). Volatile / SubPlan quals rejected. | A `WHERE dim.x` normally reduces `LEFT` to `INNER`; INNER-drop-on-miss is the correct semantics |
| G7 | **Aggregate inputs are fact-only** — no `sum(dim.col)` etc. | Phase 1 feeds transitions from fact chunks only |
| G8 | The join key **`fact.fk` is a registered columnar column** | The node reads it from the store to probe the hash |
| G9 | Group keys: each is either a fact column, a supported fact **expression** (existing `dbbc_expr_vars_ok` / non-volatile / allowed type), the **fk**, or a **dim column reachable through the join key** | Fact keys drive the hash; dim keys are attached from the probe |
| G10 | Supported aggregates (existing `dbbc_agg_trans_ok`) and group-key types (`dbbc_grp_key_type_ok`) | Same as current grouped-agg gate |
| G11 | Estimated **fact-side** group count `≤ DBBC_GRP_MAX_GROUPS` (100k), using expression statistics if present | The node hashes by fact keys; no spill (see [§7](#7-group-count--the-no-spill-cap)) |

> **Note on G9/dim keys:** in the canonical Odoo shape the dim keys are `dim.pk` (equal to `fact.fk`,
> already a fact column) and columns **functionally dependent** on it (`sequence`). The hash is keyed by
> the *fact* keys; dim attrs are carried per group.

---

## 4. Non-goals (Phase 1)

- Fan-out joins (non-unique dim key) — rejected by G4.
- ~~Dimension-side quals / filtered joins~~ — **shipped in Phase 2** (INNER only, see below).
- Aggregates over dimension columns — rejected by G7.
- Multiple dimensions / snowflake chains — rejected by G2 (Phase 2).
- RIGHT/FULL joins, non-equi joins — rejected by G3.

### Phase 2 (shipped): INNER joins + dimension-side `WHERE` filters

A dimension filter almost always arrives as an **INNER** join — `reduce_outer_joins`
collapses `fact LEFT JOIN dim … WHERE dim.x = V` before the grouping hook runs (the
`SpecialJoinInfo` disappears and `dim.x = V` lands in `dim_rel->baserestrictinfo`). Phase 2:

- **INNER detection:** accept `sji == NULL` when no non-inner special join entangles the
  fact or dim. The equi-join `fact.fk = dim.key` is recovered from the **EquivalenceClass**
  that links the two rels (inner-join equalities are absorbed into ECs and removed from
  `joininfo`, unlike outer-join ON-clauses).
- **Dim quals at build:** `dim_rel->baserestrictinfo` is compiled to an `ExprState` and
  evaluated per dim tuple in `dbbc_dim_build`; only passing rows enter the hash. Volatile /
  SubPlan quals are rejected (would diverge from a plain plan / are not modeled here).
- **Drop-on-miss:** a fact-row probe that finds no (passing) dim entry is dropped for INNER
  (vs. the LEFT NULL-extension). Applied on both the columnar-block and heap-fallback paths.
- Still deferred (**Phase 2b**): a *preserved* LEFT join carrying a dim filter (anti-join
  idiom); grouping by a dimension **non-key** column without the dim key in `GROUP BY` (a
  pre-existing limitation shared with Phase 1, orthogonal to filters).

---

## 5. Planner integration

**This is the largest and riskiest part.**

### Hook point
`dbbc_create_upper_paths` (installed on `create_upper_paths_hook`), stage `UPPERREL_GROUP_AGG` and
`UPPERREL_PARTIAL_GROUP_AGG`. Today it only proceeds when `input_rel` is a **base** columnar rel. Add a
branch: when `input_rel->reloptkind == RELOPT_JOINREL`, attempt the dim-join shape.

### Shape extraction (from `input_rel` / `root`)
1. Confirm `input_rel->relids` is exactly two base rels; classify which is the **fact** (registered
   columnar, G1) and which is the **dim** (G2).
2. Recover the join type and the single equi-clause `fact.fk = dim.key` from
   `root->join_info_list` / the join rel's `joininfo` / restrictlist (G3). Enforce fact on the outer
   side for LEFT.
3. Prove `dim.key` unique (G4) via `relation_has_unique_index_for` / catalog unique constraints.
4. Pull the fact `baserestrictinfo` (the `WHERE state=… AND company_id=…`) to push into the columnar
   scan/prefilter, exactly as the base-rel path does. Confirm **no dim-side quals** (G6).
5. Split the query's `GROUP BY` / `PathTarget` grouping columns into **fact-sourced** (Var on the fact,
   or a supported fact expression, or the fk) vs **dim-sourced** (Var on the dim reachable via the join
   key) (G9). Validate the fact side through the existing `dbbc_agg_grouped_classify` machinery.
6. Validate aggregates via `dbbc_agg_trans_ok`; confirm all agg inputs are fact Vars/exprs (G7).

### Group-count estimate (G11)
Estimate on the **fact-side** grouping expressions only (`estimate_num_groups(root, fact_keyexprs,
fact->rows, …)`). Dim attrs are FD on the fk, so they don't multiply the count. This is why the feature
needs expression statistics but **not** spill.

### Path construction
Build a `CustomPath` whose `custom_paths` carries the fact's columnar scan path and records: the dim
RTE/relid, the join type, the fk attno (fact) and dim key attno, the dim-key→dim-attr projection, and
the fact/dim split of the group keys. Cost = existing grouped-agg cost over the fact + a small per-row
hash-probe term + the dim-scan build term. `add_path` (serial) and, when parallel-safe,
`add_partial_path` for `UPPERREL_PARTIAL_GROUP_AGG`.

### Fallback
Any failed check → **return without adding a path**. Core's `HashAggregate`/`Hash Join`/eager paths
compete and win on their own.

---

## 6. Executor

Node type: reuse `DBBlueColumnarAgg` grouped mode; add a "dim-join" sub-mode.

### `DbbcAggScanState` additions
- `Relation dim_rel;`
- `TableScanDesc` (transient, build only)
- `HTAB *dim_hash;` — key = dim-key Datum bits; value = the projected dim attrs (byval Datums / a small
  MemoryContext-owned tuple for varlena)
- `AttrNumber fk_attno;` (fact) — read from the store to probe
- mapping: which output group-key slots are dim-sourced, and their source dim attnos
- `bool dim_left_join;` (LEFT vs INNER)

### Build (once, at `BeginCustomScan` / first exec)
```c
Snapshot snap = node->ss.ps.state->es_snapshot;      /* the QUERY snapshot */
dim_rel = table_open(dim_oid, AccessShareLock);      /* lock already held by the plan */
TableScanDesc s = table_beginscan(dim_rel, snap, 0, NULL);
while (table_scan_getnextslot(s, ForwardScanDirection, slot)) {
    Datum key = slot_getattr(slot, dim_key_attno, &isnull);
    if (isnull) continue;                            /* a NULL dim key never matches an equi-join */
    hash_insert(dim_hash, key -> project(dim attrs for this row));
}
table_endscan(s);
```
Using `es_snapshot` gives **identical visibility to a `Hash Join`'s inner build** — a concurrent
`UPDATE`/`DELETE`/`INSERT` on the dimension is seen iff it would be seen by a normal join. No staleness,
because the dimension is not in the column store.

### Per-row probe — the existing hook
In `dbbc_grp_consume_block` the group-key loop is already source-dispatched
(`columnar_scan.c:4359-4370`): a key comes from a fact chunk *or* a fact expression. Add a **third
source** — a dim probe — evaluated *after* the row passes the prefilter and *before* `dbbc_grp_advance`:
```c
Datum fk = dbbc_chunk_read(s, fk_col, row, &fk_isnull);
DimEntry *d = fk_isnull ? NULL : hash_search(dim_hash, &fk, HASH_FIND, &found);
for (k in dim_sourced_keys)
    keyvals[k] = (d ? d->attr[k] : (Datum) 0), keynulls[k] = (d == NULL);   /* miss → NULL (LEFT) */
```
- **LEFT join, no match:** `d == NULL` → dim keys NULL — matches PG's LEFT-JOIN NULL extension.
- **INNER join, no match:** the row is **dropped** (skip `dbbc_grp_advance`) — matches INNER semantics.
- The same probe is added to the **heap-fallback** path (`dbbc_grp_consume_heap_range`) so mixed
  columnar/heap scans stay consistent (the node has no parent recheck).

### Parallel
Each worker builds its **own** dim hash from the shared, serialized `es_snapshot` (workers already
restore the leader's snapshot). For a small dim this is cheap and needs no DSM sharing. (Optional later:
build once in the leader, publish to DSA.) The fact-side block claiming is unchanged
(`dbbc_grp_claim_slot`).

### Rescan / teardown
Rebuild is unnecessary on rescan (the dim + snapshot are stable within a query); keep the hash. Close
`dim_rel` with `AccessShareLock` at `EndCustomScan`; free the hash's MemoryContext.

---

## 7. Group-count & the no-spill cap

`DBBlueColumnarAgg` does not spill; it caps at `DBBC_GRP_MAX_GROUPS = 100000`. The dim-join node hashes
by the **fact** group keys (`fact-expr`, `fk`); the dim attrs are FD on the fk and do **not** increase
the group count. So:
- Estimate and cap on the fact-side count (G11).
- **Expression statistics are a prerequisite** for the expression-keyed Odoo shape (without them the
  planner over-estimates the expression's n-distinct — e.g. 417,818 vs a true ~5,000 — and G11 rejects).
  Document `CREATE STATISTICS ON (the expr) FROM fact; ANALYZE fact;` as the operator step, or consider
  auto-creating it when a columnar column is registered.
- **Not** coupled to spill support (that follow-up would only matter for genuinely high-cardinality
  fact grouping).

---

## 8. Correctness / MVCC summary

| Concern | Resolution |
|---|---|
| Dimension changes mid-query | Read the dim under `es_snapshot` → identical to a `Hash Join` inner build; no staleness |
| Dimension not cached in store | Correct — it never enters block validity; the M6 proof/re-stamp is untouched |
| LEFT-join NULL extension | Probe miss → NULL dim keys (LEFT); drop row (INNER) |
| Fan-out (dup dim keys) | Impossible by G4 (unique key) |
| Fact-side visibility | Unchanged: all-visible columnar blocks are visible to all snapshots; heap-fallback uses `es_snapshot` |
| Snapshot consistency fact vs dim | Both use `es_snapshot` → one consistent view |

---

## 9. Test matrix

All correctness tests are **differential**: same query with the feature on vs off must be
**byte-identical**, run inside one `REPEATABLE READ` transaction so both see one snapshot (the
`agree()` idiom already in the suite).

| Area | Cases |
|---|---|
| Join type | INNER; LEFT (fact outer) |
| FK matching | all matched; some `fk` with no dim row (LEFT→NULL, INNER→drop); `fk IS NULL` |
| Uniqueness | dim key PK/unique → feature fires; non-unique → **must fall back** (assert plan) |
| Group keys | fact column; fact expression (`date_trunc`/timezone); fk; dim column; mix |
| Aggregates | `count(*)`, `count(col)`, `sum`, `avg`, `min`/`max`, `stddev`; `FILTER (WHERE …)`; fact-only inputs |
| Snapshot | concurrent `UPDATE dim SET …` committed after snapshot → result unchanged; before → reflected |
| Size cap | dim over `dimjoin_max_dim_rows` → fall back to PG plan |
| Dim filter | any dim-side qual → fall back (Phase 1) |
| Execution mix | fully columnar blocks; forced heap-fallback rows; mixed (probe consistent across both) |
| Parallel | serial vs 2+ workers identical; per-worker hash correctness |
| Degenerate | zero matching fact rows; zero-row groups; rescan under nested loop |
| Plan assertions | `uses_node(...)` confirms the fused node fires only when eligible, falls back otherwise |

Plus an **assert-enabled concurrent stress run** (the `restamp_stress` harness pattern): readers
running the report while writers mutate both fact and dim + vacuum + populate, with a RR correctness
oracle; expect zero crashes/asserts and zero mismatches.

---

## 10. Risk table (re-scored)

| Area | Risk | Note |
|---|---|---|
| MVCC / snapshot | 🟢 | `es_snapshot` dim build = a hash-join inner build |
| Memory | 🟢 | Hard `dimjoin_max_dim_rows` cap → fall back |
| NULL / LEFT-join semantics | 🟡 | Must test (probe miss → NULL / drop) |
| Duplicate dim keys | 🟢 | Enforced unique by G4 |
| Aggregate over dim cols | 🟢 | Excluded by G7 |
| **Planner pattern-match** | 🔴 | The real work — join-tree extraction, key split, cost |
| Group-count estimate | 🟡 | Needs expression statistics (documented / auto) |
| General Odoo compatibility | 🟢 | Strict gate + fallback |

---

## 11. Phasing

1. **Phase 1** — the strict gate above, behind `enable_dimjoin_agg` (default off). One fact, one small
   unique-key dim, INNER/LEFT equi-join, fact-only aggregates, no dim quals. Full test matrix + stress.
2. **Phase 2** — multiple dimensions (chain of probes); relax to allow simple immutable dim-side quals
   folded into the build scan; optional DSA-shared dim hash for large-but-capped dims.
3. **Later** — spill support (decouples the fact-side cap), auto expression-statistics on registration.

---

## 12. Decisions (finalized 2026-08-21)

- **D1 — Operator-driven + documented.** Registration does **not** auto-mutate planner statistics.
  The expression-statistics prerequisite is explicit (documented + a helper), avoiding surprising
  planning/ANALYZE overhead.
- **D2 — GUC `dbblue_columnar.dimjoin_max_dim_rows`, default 65536.** Configurable cap; conservative
  default (Odoo/custom dimension tables vary widely).
- **D3 — Per-worker dimension hash for Phase 1.** Dim is capped at ≤65,536 rows, so duplicating a small
  hash per worker beats DSA lifecycle/synchronization complexity. Revisit only if profiling shows it.
- **D4 — LEFT-only first.** The dominant Odoo shape; clearer fact-row-preserving semantics. Add INNER
  after the LEFT differential tests pass.
- **D5 — Sub-mode of `DBBlueColumnarAgg`.** Reuse the existing block traversal, validity/re-stamp,
  column access, grouping, transition, instrumentation, and fallback — add a join-aware mode, not a new
  executor node.
