# DBblue IVM — Progress Log & Backlog

Living status for the incremental materialized view (IVM) work. Open this first
to see what is done and what is next. Companion docs:
- Architecture → `INCREMENTAL_MATVIEW.md`
- Supported shapes + roadmap → `INCREMENTAL_MATVIEW_SUPPORT_AND_ROADMAP.md`
- Phase 2 deparse-core design → `INCREMENTAL_MATVIEW_PHASE2_DESIGN.md`

_Last updated: 2026-06-18 · branch `feature/ivm-incremental-refresh`_

---

## Most recent work: Phase 2 step 2 — expression aggregate args (auto-routed)

Made expression aggregate arguments — `SUM(CASE …)`, `AVG(COALESCE(…))`,
immutable function calls — incrementally maintainable **and restorable**, by
auto-routing such shapes to the deparse core independent of the GUC.

**What it does**
- `incr_plain_agg_needs_deparse(viewQuery)` — true when any aggregate argument
  is outside the hand grammar. Generation now uses deparse when
  `dbblue_ivm_deparse_delta || incr_plain_agg_needs_deparse(...)`, so the shape
  is routed the same way at CREATE and at restore → **no dump/restore footgun**.
- Eligibility relaxed **only for the plain single-table aggregate shape**
  (`nbasetables==1 && GROUP BY && !HAVING && !MIN/MAX`): an aggregate arg is
  accepted if the hand grammar accepts it **or** it is `incr_agg_arg_deparse_safe`
  (deterministic — no nested agg/window/subquery, and **immutable**, so the
  insert-delta and a later delete-delta cancel exactly). The union never narrows
  what was accepted before.
- Other shapes (JOIN, MIN/MAX, HAVING) keep the restricted hand grammar until
  deparse is widened to them.

**Safety rails (all rejected at CREATE):** volatile/stable args (would drift),
nested agg/window/subquery args, and expression args in non-deparse shapes.

**Proof:** full `.sql` suite off+on; dump/restore under the **default GUC** now
includes an auto-routed `SUM(CASE)+AVG(COALESCE)` matview (`mv_expr`) that
survives restore and is correct; concurrency green. New test
`src/test/dbblue_ivm/phase2_expr_aggregates.sql` (4 PASS).

---

## Earlier this work: Phase 2 step 1 — deparse delta core

Landed the query-tree **deparse** path for the plain single-table aggregate
shape, behind a default-off GUC, beside the existing engine and proven
equivalent to it.

**Commits**
- `ea92a4e` — Phase 2 foundation: ENR-aware `get_query_def` + `dbblue_deparse_query`
- `04fb69c` — Phase 2 design doc
- `44d864e` — Phase 2 step 1: deparse delta core for plain aggregates

**What it does**
- `incr_build_delta_select_query` copies the stored view `Query` and swaps the
  source relation RTE → its transition-table ENR (`__mv_newtable` /
  `__mv_oldtable`), mirroring `addRangeTableEntryForENR`. `dbblue_deparse_query`
  then renders the delta SELECT — so expressions, aliasing, and NULL semantics
  are handled by PostgreSQL itself.
- INS/DEL **shells** factored out of the hand builders
  (`incr_emit_ins_head` / `incr_emit_ins_conflict_tail` /
  `incr_emit_del_update_tail`): both the hand and deparse paths now share
  byte-identical INSERT-head + `ON CONFLICT` accumulate + `UPDATE…FROM d` merge
  logic. Only the SELECT body differs.
- Gated by GUC `dbblue_ivm_deparse_delta` (default off, `DEVELOPER_OPTIONS`).
  **MIN/MAX and HAVING bypass deparse** — their delta SELECT must not be a
  literal render of the view query (HAVING would wrongly filter the delta;
  MIN/MAX needs its two-phase rescan).

**Correctness fix shipped alongside (affects the default path):** the hand
deparser treated *every* single-argument `FuncExpr` as a cast, silently
dropping `floor()` / `abs()` and corrupting the running total
(`SUM(floor(amt))` was maintained as `SUM(amt)`). Now genuine single-arg
functions render as calls; casts only when `funcformat` says so.

**Proof (all green)**

| Check | GUC off | GUC on |
|---|---|---|
| Full `dbblue_ivm` `.sql` suite | ✅ | ✅ |
| dump/restore (catalog round-trip + re-arm) | ✅ 16/16 | ✅ 16/16 |
| RR + SERIALIZABLE concurrency | ✅ | ✅ |
| TRUNCATE concurrency | ✅ | — |
| New `phase2_deparse_delta.sql` | ✅ 3/3 | ✅ 3/3 |

New test: `src/test/dbblue_ivm/phase2_deparse_delta.sql`.

---

## Done so far (cumulative)

### Phase 1 — engine (production-ready for its envelope)
- AFTER-STATEMENT triggers + precomputed delta SQL in `pg_dbblue_matview`.
- Counting algorithm (`__mv_count__`); AVG maintained as a (sum, count) pair;
  MIN/MAX via two-phase advisory-lock rescan.
- Shapes: single-table aggregate, multi-table INNER/LEFT/RIGHT/FULL & CROSS
  JOIN, row-level (no GROUP BY), DISTINCT, HAVING, UNION ALL (uncertified
  under concurrency), WHERE filters, CTE/subquery normalization.
- Correctness: Bug A (aliased MIN/MAX), Bug B (NULL aggregate arg), Bug C
  (NULL group key → auto-exclude `IS NOT NULL`, never blocks a write).
- Hardening: TRUNCATE, dump/restore re-arm, HAVING teardown, float-aggregate
  rejection, DDL-on-source guards + uniform hints, DISTINCT/FILTER/GROUP-BY-expr
  rejection.
- Guarantee: normal (non-incremental) matviews are entirely unaffected.
- Tests: `src/test/dbblue_ivm/` (now 10 `.sql` + 3 `.sh`).

### Phase 2 — query-tree deparse redesign
- Foundation: ENR-aware `get_query_def` + `dbblue_deparse_query` (proven).
- Step 1: plain single-table aggregate via deparse, behind the GUC.
- **Step 2: expression aggregate args (`SUM(CASE)`, `COALESCE`, immutable
  functions) auto-routed to deparse — maintainable and restorable.**

---

## What's left to do

### Next increment — widen the deparse gate shape-by-shape (strangler)
Migrate each shape to deparse and delete its hand builder once equivalent:
1. **JOIN** (multi-table). Fix the latent ENR-name-vs-refname issue here: a
   deparsed `Var` qualifies with the RTE refname, which may differ from the ENR
   name; the emitted relation reference and the Var qualifier must agree.
2. **MIN/MAX** — keep the two-phase rescan; deparse only the scan SELECT.
3. **HAVING** — strip `havingQual` from the delta copy (the delta must not
   apply HAVING), keep `__mv_having_ok__` + `hav_sql` recompute.
4. **UNION ALL** — and revisit its concurrency certification.
- Each step: prove equivalence (suite + dump/restore + concurrency) with the
  GUC off and on, then remove the superseded hand builder.

### Remaining correctness fidelity
- Full NULL-group fidelity: maintain NULL group keys instead of excluding them
  (`NULLS NOT DISTINCT` unique index + `IS NOT DISTINCT FROM` joins), removing
  the documented divergence from a normal `REFRESH`.
- Exact all-NULL `SUM`: a per-column non-null counter so an all-NULL group
  shows SQL `NULL`, not `0` (current accepted residual; see `incr_nullsafe_accum`).

### Eventually
- Flip the deparse path on by default once every shape is migrated and the hand
  builders are deleted; retire the GUC (or keep it as an escape hatch).
- Broaden Odoo report coverage per `INCREMENTAL_MATVIEW_SUPPORT_AND_ROADMAP.md`
  §6 (window functions, nested aggregation, etc. — where feasible).

---

## How to run the proof locally
```bash
# from repo root; server on port 5432, data dir new_work_ivm, user cybrosys
for f in src/test/dbblue_ivm/*.sql; do bin/psql -p 5432 -U cybrosys -d postgres -q -f "$f"; done
PGOPTIONS="-c dbblue_ivm_deparse_delta=on" \
  for f in src/test/dbblue_ivm/*.sql; do bin/psql -p 5432 -U cybrosys -d postgres -q -f "$f"; done
bash src/test/dbblue_ivm/dump_restore_consistency.sh        # off
PGOPTIONS="-c dbblue_ivm_deparse_delta=on" bash src/test/dbblue_ivm/dump_restore_consistency.sh
bash src/test/dbblue_ivm/isolation_levels.sh 12 1
```
