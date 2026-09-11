# DBblue IVM — M4 Design: FROM-subqueries, CTEs, correlated sublinks

**Thesis:** M4 is a **normalization front-end**, not a change to the maintenance
engine. Simple FROM-subqueries and CTEs are *flattened at CREATE time* into the
equivalent flat query over base relations that the current unified engine already
maintains. The delta/recompute/trigger/deparse core is **untouched** for M4.1/M4.2.

> This document is the output of the M4 design phase (mapped against the current
> code + an adversarial approach comparison). It reflects the engine as of the
> optional-side safety fix (`1e9ddaeb527`). Verify loci before coding — the tree
> moves.

---

## Why M4, why now

Real-Odoo validation (DB `odoo`, 18 report views) measured direct coverage at
**1/18 = 5.6%**, with a perfect clean-rejection record. The rejections cluster,
and the **single largest bucket is FROM-subqueries / CTEs** (5/17). Every
"only plain table references are supported" rejection is literally a
`FROM (SELECT …)` derived table or a `WITH` CTE. M4 alone → ~6/18 = **33%** (6×).
See `project_odoo_validation_coverage` (memory) and `SUPPORTED_SQL_SHAPES.md`.

---

## The strategy: normalize-then-reuse (chosen; scored 8.5/10 vs 6 / 5)

Three integration approaches were designed and judged:

| approach | score | verdict |
|---|---|---|
| **A — Normalize then reuse** (flatten at CREATE, engine unchanged) | **8.5** | **chosen** |
| B — Native subquery recursion (engine descends into subquery RTEs) | 6 | over-engineered for M4's shapes; ~40-consumer blast radius; relaxes the gate backstop |
| C — Sub-matview materialization (inner subqueries as their own matviews) | 5 | poor fit for *simple* subqueries (they should flatten); large new lifecycle surface. One idea kept: materialize-once for multiref CTEs |

### The correctness spine (why this is safe)

A full `REFRESH` runs the original view query through `subquery_planner`, which
calls **`pull_up_subqueries`** (`prepjointree.c`). So if the CREATE-time flatten
admits **only** what `is_simple_subquery` would flatten and performs the **same
mechanical transform**, then:

```
REFRESH(flattened) ≡ REFRESH(original)          [PostgreSQL's own planner does this]
maintain(flattened) ≡ REFRESH(flattened)        [the proven flat-engine invariant]
⇒ maintain(flattened) ≡ REFRESH(original)       [byte-identity, inherited — not newly proven]
```

Equivalence is **inherited from the planner**, not re-derived. That is the whole
appeal over B and C, whose `== REFRESH` arguments would be newly minted.

### The one governing discipline

**Never relax `MatviewIncrIsEligible`.** The gate (matview_incr.c:**720** hasSubLinks,
**762** cteList, **822–827** non-`RTE_RELATION`) stays exactly as strict as today.
M4 only **widens the pre-pass normalizer that runs *before* the gate**. Anything
the normalizer cannot losslessly flatten keeps its `RTE_SUBQUERY` / `RTE_CTE` /
`SubLink` and is **rejected cleanly** at the unchanged gate. Consequence: a
partially-flattened or non-equivalent query can **never** reach
`incr_collect_tables` (matview_incr.c:5683–5742), where an `RTE_SUBQUERY` leaf
would synthesize an `oid = 0` `IncrJoinEntry` and silently corrupt every builder.
**That backstop is the entire safety story.**

### What already exists (build on, don't invent)

There is already a CREATE-time normalizer: `MatviewIncrNormalize`, wired at
`createas.c`, with a T1 filter-inline splice (`incr_try_inline_filter`) and
substitution mutators (`incr_subst_col` / `incr_apply_subst_col`), plus a CTE
path (`incr_try_normalize_cte`). Today T1 is gated to the **single-base** case
(`incr_single_base_varno(q) > 0`, ~10818–10823). **M4.1 generalizes that gate
from single-base to a multi-table splice.** The scaffolding, dispatch, and the
stored-query sync at `createas.c` are already there.

### The pipeline

```
              user's CREATE MATERIALIZED VIEW … (query with FROM-subqueries/CTEs)
                                   │
   NEW / WIDENED  ▼  ┌───────────────────────────────────────────────────────────┐
                     │ MatviewIncrNormalize (pre-pass, createas.c):                │
                     │   • inline non-recursive CTEs      (incr_try_normalize_cte) │
                     │   • splice simple FROM-subqueries  (generalized T1)         │
                     │   → equivalent FLAT query over base RTE_RELATIONs           │
                     └───────────────────────────────────────────────────────────┘
                                   │  (residue that can't flatten keeps its RTE_SUBQUERY/CTE/SubLink)
                    UNCHANGED  ▼
                     MatviewIncrIsEligible  — flat query passes; residue rejected cleanly
                    UNCHANGED  ▼
                     incr_collect_tables → trigger install → RTE-swap delta / recompute engine
```

---

## Phased plan

### M4.1 — Simple multi-table FROM-subqueries  *(Medium, days)*

**Deliverable.** A non-lateral FROM-subquery whose body is filter / projection /
explicit-INNER-JOIN over base relations (**no** agg / group / distinct / having /
window / SRF / limit / setop / sublink; **no** volatile tlist function), placed on
a **non-nullable** side, is flattened at CREATE and maintained by the unchanged
engine. This is the dominant "multi-table SPJ subquery joined to dimensions" Odoo
report shape.

**Key changes.**
1. Generalize `incr_try_inline_filter` from the single-base path to a **multi-table
   varno-offset splice** mirroring `pull_up_simple_subquery` **without**
   `PlaceHolderVar`/`AppendRelInfo`: `OffsetVarNodes` the inner subtree by
   `list_length(outer->rtable)`, append inner `rtable` + `rteperminfos`, splice the
   inner `FromExpr`/`JoinExpr` in place of the subquery `RangeTblRef`, and rewrite
   each outer `Var(sq_varno, K)` to the offset inner target-list expr `K`.
2. **Add `contain_volatile_functions(inner targetList)` to the inner-body gate**
   (verified missing from `incr_q_is_filter_proj`, ~10740).
3. **Post-splice shape validation:** the merged jointree must be a left-deep tree
   of explicit `JoinExpr`/`RangeTblRef` leaves, else clean reject — so
   `incr_collect_tables_recurse` never `elog`s at maintenance time.
4. Restrict the splice to **non-nullable (inner-join / preserved-side) placement**
   → no `varnullingrels`/PHV bookkeeping needed.
5. **Fix `createas.c` sync** to also copy `rteperminfos` and `hasGroupRTE` (verified
   absent) — prefer a whole-structure copy over the enumerated field-copy.

**Gate relaxation:** *none.* Post-flatten the query is flat `RTE_RELATION`.

### M4.2 — Simple CTEs (non-recursive, single- & multi-reference)  *(Small–Medium)*

**Deliverable.** Non-recursive `SELECT` CTEs with an engine-eligible body:
single-ref via the M4.1 splice; multiref via **materialize-once** (grafted from C)
or clone-per-site.

**Key changes.** Reuse `incr_try_normalize_cte` (converts by ctename, deletes from
`cteList` after inline). Single-ref → M4.1 splice. Multiref → detect refcount and
either **materialize the body once** into an internal `relkind='r'` relation
referenced N times (the existing self-join OID merge, `done_oids` 1677–1698,
collapses the N references into one delta arm) or clone per site. Re-reject
`WITH RECURSIVE`, writable/DML CTEs (`contain_dml`), and multiref CTEs with
volatile functions.

**Gate relaxation:** *none.* Normalization sets `cteList = NIL`; residual
un-inlinable CTEs keep it non-NIL and trip the 762 reject cleanly.

### M4.3 — Correlated sublinks via decorrelation (narrow)  *(Large)*

Two disjoint, provably-equivalent subsets, via an **approach-independent
decorrelation front-end that runs before normalization**:
- **(a) correlated `EXISTS`/`NOT EXISTS`/`IN` with equality correlation** →
  semi/anti-join over base relations → **flattenable, rides M4.1**. Low risk
  (standard semijoin equivalence). **Ship this subset first.**
- **(b) equality-correlated scalar-*aggregate* sublinks in SELECT** → rewritten to
  an uncorrelated grouped LEFT-JOIN derived table → **depends on M4.4** to be
  maintainable (it is an aggregating derived table). Until M4.4, decorrelate-then-
  reject (or full-REFRESH fallback). High risk — cardinality (exactly-one-or-NULL),
  `>1`-row error timing, no-rows→NULL, `NOT IN` NULL semantics.

**Gate relaxation:** narrow & surgical — the hasSubLinks reject (720) relaxed
**only** for the two decorrelatable classes, and only after the front-end has
rewritten them. All other SubLink shapes keep the 720 reject.

### M4.4 — Aggregating / DISTINCT derived tables + perf  *(Medium–Large, open-ended)*

The remaining big Odoo shape: a derived table (or CTE) that itself
`GROUP BY`/`DISTINCT`s, joined to dimensions — the class A can *never* flatten.
**Decide the mechanism on empirical evidence** in a dedicated design spike:
- **(B) native recursive-delta** — add a derivation-path to `IncrJoinEntry`, teach
  `incr_collect_tables_recurse` to descend, add
  `incr_build_delta_select_query_at_path` reusing the existing swap block; maintain
  the inner subquery's output-delta with its own `__mv_count__`/SUM and feed it as
  the outer's input delta. *Relaxes the gate; ~40-consumer chokepoint refactor.*
- **(C) sub-matview** — extend M4.2 materialize-once to aggregating inners
  (internal `relkind='r'` relations, maintained by the existing engine, referenced
  as ordinary `RTE_RELATION` srctables). *Preserves the no-gate-relaxation property;
  adds nested-trigger chaining + lifecycle + a cross-layer overlap gate.*

Also: verify the cost router + index-driven recompute (`28d1c222ceb`) +
anchor-restrict handle wider/deeper flattened joins; dedup identical materialized
bodies.

---

## Impact on triggers / delta / recompute

**M4.1/M4.2: essentially ZERO change** — the point of flatten-then-reuse.
- **Base-table discovery:** after a splice, every inner base table is a *top-level*
  `RTE_RELATION` with its real `relid`; `incr_collect_tables` reads
  `entry->oid = rte->relid` legitimately (no `oid=0` poisoning — non-flattenable
  subqueries never reach discovery). *New obligation:* the splice must emit a
  left-deep tree (post-splice shape validation).
- **Trigger install:** unchanged. `incr_install_triggers` is OID-keyed; a flattened
  inner table is a top-level `RTE_RELATION` with a real OID. Dispatch
  (`matview_delta_apply`, `(mvrelid, srctable)` lookup) is unchanged; nesting was
  dissolved at CREATE.
- **RTE-swap delta:** unchanged. `incr_build_delta_select_query` swaps a top-level
  `RTE_RELATION` for an ENR — the flattened target *is* one. The one interaction: a
  base table appearing at 2+ varnos (multiref CTE / same table flattened + top-level)
  is absorbed by the **existing self-join OID merge** (`done_oids`). Watch the
  diamond/3+-occurrence reject (812) — materialize-once sidesteps it.

**M4.4** is where the machinery genuinely changes (per chosen mechanism above).

---

## Must-resolve before coding

1. **createas.c sync completeness** — copy `rteperminfos` + `hasGroupRTE` (or switch
   to whole-structure copy) so stored ≠ execution query can never desync.
2. **Outer-join placement test** — the exact conservative check that a subquery's
   `RangeTblRef` slot is **not** on any outer join's nullable side (including nesting
   under a higher outer join). Getting this wrong risks the silent-wrong-all-NULL-
   group class — the same machinery hardened in `1e9ddaeb527`.
3. **Multiref CTE strategy** — clone-and-splice (can trip the 3+-occurrence reject)
   vs materialize-once (imports DROP-cascade / pg_depend-hide / REFRESH-ordering).
   What refcount/complexity threshold picks between them?
4. **Planner-parity drift** — `is_simple_subquery` / `contain_dml` are static in the
   planner and must be mirrored. Co-locate the mirror with a **REFRESH-oracle
   differential regression test** so a rebase can never let us flatten something
   `REFRESH` would not.
5. **M4.4 mechanism (B vs C)** — empirical spike first. For C, prove nested
   AFTER-STATEMENT triggers + `INSERT…ON CONFLICT` + final vanished-group DELETE
   present *exactly* the net per-statement delta. For B, the true cost of the
   `IncrJoinEntry` chokepoint refactor + `varnullingrels`-through-nesting.
6. **M4.3 provable subset** — ship (a) EXISTS/IN semi/anti-join in M4.3; gate (b)
   scalar-aggregate behind M4.4.
7. **Clean-reject audit** — confirm every un-spliceable shape (implicit-comma inner
   FROM, post-flatten unsupported outer-join/diamond topology, non-left-deep tree) is
   rejected at CREATE, never at a maintenance-time `elog` or as an `oid=0`
   `IncrJoinEntry`.

## Testing

Every flattened shape must be differentially validated **against a full REFRESH of
the original (un-flattened) query** — that is the oracle that proves the mechanical
splice matched the planner's pull-up. Add these to `src/test/dbblue_ivm/` alongside
a permanent `is_simple_subquery`-parity check.
