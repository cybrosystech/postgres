# DBblue IVM — Progress Log & Backlog

Living status for the incremental materialized view (IVM) work. Open this first
to see what is done and what is next. Companion docs:
- Architecture → `INCREMENTAL_MATVIEW.md`
- Supported shapes + roadmap → `INCREMENTAL_MATVIEW_SUPPORT_AND_ROADMAP.md`
- Phase 2 deparse-core design → `INCREMENTAL_MATVIEW_PHASE2_DESIGN.md`

_Last updated: 2026-06-24 · branch `feature/ivm-incremental-refresh`_

---

## Most recent work: GROUP BY on an expression (e.g. `date_trunc`)

Time-bucketed and derived-key reports — `GROUP BY date_trunc('month', d)`,
`(amt % 10)`, `CASE …`, `lower(name)`, `(a + b)` — are now maintained
incrementally. The deparse core already copies the view Query (grouping
expressions and all) and only swaps the source RTE for the transition table, so
ruleutils renders the same GROUP BY over the changed rows; every consumer keys on
the matview OUTPUT column that stores the expression value. So this was mostly an
eligibility + routing change, not new delta machinery.

- **Eligibility:** a non-Var GROUP BY key is accepted iff (1) the shape is one the
  deparse core builds (single-table or INNER JOIN, no MIN/MAX, no self-join),
  (2) the expression is IMMUTABLE and free of subqueries/aggregates/window funcs
  (`incr_agg_arg_deparse_safe` — a STABLE/volatile key could map a row to
  different groups on its insert- vs delete-delta and drift), and (3) it appears
  in the SELECT list (an output column to store + key on). `incr_group_key_expr`
  resolves the PG17+ RTE_GROUP indirection.
- **Routing:** `incr_group_needs_deparse` forces the deparse path for expression
  keys regardless of the GUC (the hand builders can't render them), keeping them
  restorable.
- **Adversarial audit** (3 rounds, 200+ shapes vs full `REFRESH`) found and fixed
  4 issues, all in the HAVING path / validation rather than the delta:
  1. HAVING that references the group EXPRESSION hit an internal "non-Var in
     groupexprs" error — now binds the HAVING group key to its matview output
     column by RTE_GROUP slot (also fixes aliasing).
  2. `HAVING <expr> IS [NOT] NULL` (NullTest) — added to the HAVING deparser.
  3. `HAVING power(x,2) > 1` (multi-arg / real function) — the deparser treated
     every 1-arg function as a cast and errored on multi-arg; now renders casts
     vs calls correctly at any arity (mirrors the WHERE deparser).
  4. A user output column named `__mv_count__` collided with the hidden count
     column (zero-count cleanup dropped key-≤0 groups) — now rejected at CREATE
     with a clear "reserved __mv_ prefix" message, while restore (which carries
     `__mv_count__` as COUNT(*)) is still accepted.
  The HAVING validator and deparser are now grammar-aligned (Var, Const, Aggref,
  NullTest, OpExpr, BoolExpr, FuncExpr any-arity, RelabelType).
- **Tests:** `group_by_expr.sql` — single-table / arithmetic / mixed plain+expr /
  HAVING (incl. the 4 regressions) / INNER JOIN, NULL bucket, on both GUC paths,
  all == full `REFRESH`; plus the rejections. dump/restore re-arm verified for
  plain and HAVING expression-grouping matviews.

---

## READ COMMITTED safety for the recompute/multiset shapes

The recompute/overwrite and multiset shapes — row-level (no GROUP BY) projections,
UNION ALL, outer join, self-join, and MIN/MAX — now stay consistent with a full
`REFRESH` under concurrent writers **at every isolation level, READ COMMITTED
included** (RC is the default and Odoo's level). They previously diverged at RC
because they read a region under one snapshot and overwrote it, so a concurrent
committed write could be lost.

The fix is a **matview-level advisory lock** (`incr_build_mv_lock_sql` →
`pg_advisory_xact_lock(mvrelid)`), stored as the catalog `lock_sql` for exactly
these shapes and run by `matview_delta_apply` **as its own SPI statement before
the INSERT delta**. Being a *separate* statement is the crux: a concurrent
maintainer blocks there until the holder commits, and the delta statements that
follow then take fresh READ-COMMITTED snapshots that already include the
committed change — so the recompute can't lose an update. (Embedding the lock in
the delta statement would not work: RC fixes that statement's snapshot *before*
the lock is acquired.)

- **Additive shapes stay lock-free.** Single-table and INNER JOIN SUM/COUNT/AVG
  store `NULL` for `lock_sql` and skip the step — their `ON CONFLICT` upserts
  already serialize on the matview row lock and compose correctly at RC, so they
  keep full per-group write concurrency.
- **MIN/MAX** moved from its per-group two-phase del-lock to this matview-level
  lock (run before INS); the dead `incr_build_minmax_lock_sql_gen` was removed.
- The CREATE-time message dropped from a correctness **WARNING** to an
  informational **NOTICE**: *"<shape>; its maintenance is serialized under
  concurrent writes"* — accurate now that the only cost is serialized
  maintenance, not possible divergence.

Verification:
- `concurrency_exotic.sh` — every shape now **gated at READ COMMITTED and
  REPEATABLE READ** (was: RC informational-only for the recompute shapes).
  3× clean runs.
- `isolation_levels.sh` — added a **READ COMMITTED** round: SUM + MIN/MAX
  consistent with **0 retries / 0 failures** (writers block on the lock rather
  than aborting), plus the existing RR + SERIALIZABLE rounds.
- Full `.sql` suite, `dump_restore_consistency.sh`, `truncate_concurrency.sh`
  all green.

---

## Earlier this work: NULL-group fidelity (bug E)

NULL and partial-NULL group keys are now **maintained with full fidelity**
(== a full `REFRESH`) for every shape whose delta goes through the shared shells:
single-table & INNER JOIN aggregates, DISTINCT, and HAVING. Previously a row
with any NULL key was excluded — fine for a single key, but a multi-column key
like `(5, NULL)` was wrongly dropped though a REFRESH keeps it as its own group.

- `incr_create_unique_index` → `NULLS NOT DISTINCT`, so a NULL/partial-NULL key
  is a single `ON CONFLICT` arbiter row (otherwise NULL keys never conflict and
  the upsert piles up duplicates). Identical to before for non-NULL keys.
- `incr_emit_del_update_tail` (shared by hand + deparse DELETE) → key join uses
  `IS NOT DISTINCT FROM` so a NULL key matches its delta row.
- `MatviewIncrAddNotNullKeyFilters` now injects the `IS NOT NULL` exclusion
  **only** for MIN/MAX and self-join shapes (whose hand delta SQL still matches
  keys with `=`/`USING`/`IN`/`hashtext`); every other shape keeps NULL keys.
  Writes are still never blocked in any case.

**Proof:** rewrote `null_key_exclusion.sql` → NULL-group **fidelity** (NULL group
maintained == REFRESH; multi-key partial-NULL == REFRESH; MIN/MAX still excludes
+ write not blocked; NOT-NULL regression). `audit_regressions.sql` BUGE; full
suite, dump/restore, and RR/SERIALIZABLE concurrency green off+on.

**Still excluded by design (documented):** MIN/MAX and self-join NULL keys.

### Follow-up: all-NULL SUM shows SQL NULL (not 0)
`SUM(x)` over a group whose inputs are all NULL is SQL NULL; incrementally it
used to settle to `0` once the last non-NULL was removed. Fixed for the
shared-shell shapes (single-table, INNER JOIN, HAVING; hand + deparse) with a
hidden per-SUM non-null counter `__mv_sumcnt_<col>` (added by `AddCountTarget`,
gated to non-MIN/MAX, non-self-join): the visible SUM is rendered
`CASE WHEN sumcnt=0 THEN NULL ELSE running_sum END`, and the running sum
recovers from NULL when a non-NULL input returns. MIN/MAX and self-join keep the
`0` residual (they don't go through the shared shells). Test:
`null_sum_fidelity.sql`; suite/dump/restore/concurrency green off+on.

---

## Earlier this work: audit round 2 — exotic-shape bugs

A second adversarial audit targeted the shapes still on **hand builders** (outer/
self joins, UNION ALL, row-level, MIN/MAX-over-join, DISTINCT). The aggregate
core stayed clean again — **MIN/MAX-over-join, MIN/MAX edge cases, and LEFT JOIN
all verified clean**, confirming the round-1 MIN/MAX fix holds. Five real issues
were found in the exotic shapes; one fixed, four documented as known gaps.

**Fixed — row-level duplicate multiplicity (bug A):** a row-level (no GROUP BY)
matview keeps duplicate rows, but the DELETE matched by value and removed *all*
identical copies instead of the deleted multiplicity (single-table and join).
Rewrote the row-level DELETE to drop exactly `k` copies per tuple via
`row_number()`+`ctid`. Verified == REFRESH (multiset); regression in
`audit_regressions.sql`.

### Exotic-shape bugs from round 2 — D, B, C fixed; E deferred
- ✅ **D — self-join self-referential DML aborted the user's write** ("ON
  CONFLICT … cannot affect row a second time", from the `ΔR⋈ΔR` double-count).
  Fixed (`72fdd4e`): replaced the two role-arms with a recompute-of-affected-
  groups apply — one upsert per key, correct by construction, never aborts a
  write.
- ✅ **B — UNION ALL dropped duplicate multiplicity** (deduped into one row +
  `__mv_count__`). Fixed (`00b1cec`): maintained as a plain multiset — plain
  INSERT keeps duplicates, multiplicity-respecting DELETE; no count/dedup/index.
- ✅ **C — FULL OUTER JOIN stale NULL-extended phantom** on the
  unmatched→matched transition. Fixed (`5b38733`): the sync-region DELETE now
  mirrors the INSERT region (delete by both sides' join keys).
- ✅ **E — multi-key NULL over-exclusion** — fixed (NULL-group fidelity; see the
  section above). NULL/partial-NULL keys are maintained for the shared-shell
  shapes via a `NULLS NOT DISTINCT` index + `IS NOT DISTINCT FROM` joins; MIN/MAX
  and self-join still exclude NULL keys (documented).

All confirmed wrong-answer/write-abort bugs from both audits are now fixed.
Regressions for every fix live in `audit_regressions.sql` (8 cases, each vs a
full `REFRESH` or a clean rejection).

---

## Earlier this work: adversarial audit + 3 correctness fixes

Ran an adversarial correctness audit (workflow: 8 dimensions, each probing with
real SQL against the full-`REFRESH` oracle, then independent verification). The
deparse core came back **clean** (expression/HAVING/lifecycle dimensions found
nothing); all confirmed bugs were in the **hand builders**. Three fixed:

1. **MIN/MAX corrupted co-located SUM/COUNT/AVG** (critical, silent wrong answer,
   both GUC states). A matview with MIN/MAX *and* SUM/COUNT(arg)/AVG over the
   same column lost SUM/COUNT when an argument went NULL→value (or a group's last
   non-NULL was removed then re-added). Causes: the MIN/MAX INS `upd` used plain
   `+` (so `NULL + delta = NULL`) instead of the NULL-safe accumulator; and the
   DEL subtracted `COUNT(*)` from `COUNT(arg)`/`avgcnt` columns (which exclude
   NULLs). Fixed: NULL-safe INS accumulation + a proper `COUNT(arg)` delta column.
2. **3+-table INNER JOIN broadcast a single-row delta to other groups** (hand
   join path; the deparse path was already correct). Fixed by routing **all pure
   inner-join aggregates to the deparse core** (GUC-independent), retiring the
   buggy hand join-delta for these shapes.
3. **LEFT/RIGHT/FULL self-join** leaked an internal catalog unique-constraint
   error at CREATE (fail-closed). Now rejected cleanly (`feature_not_supported`).

Not bugs (correctly dismissed by verification): a cosmetic numeric display-scale
difference (values equal), and a REFRESH-side snapshot-timing test artifact.

**Proof:** new `audit_regressions.sql` (the three minimized repros, vs REFRESH /
clean reject); full suite, dump/restore, and RR/SERIALIZABLE concurrency green
off+on.

---

## Earlier this work: JOIN + HAVING via the deparse core

Extended HAVING support to pure INNER JOINs — including expression aggregates
over the join (e.g. `… JOIN … GROUP BY p.categ HAVING SUM(CASE …) > X`).

- Dropped the `!hasHaving` gate from the JOIN deparse branch; a single
  `used_deparse` flag (`!self-join && !MIN/MAX && (GUC || needs-deparse)`) drives
  both the per-table delta builders and the failing-group backfill.
- The deparse backfill (`incr_build_backfill_sql_deparse`) already deparses over
  the real base relations, so it renders the join with no change.
- Routed the **restore**-path JOIN backfill (`MatviewIncrPostRefresh`, ≥2-table
  branch) the same way, so JOIN + HAVING is restorable.
- `deparse_agg_shape` now allows HAVING for the INNER-JOIN shape too.

**Proof:** gold-standard oracle → **13 shapes** incl. plain JOIN+HAVING and
JOIN+`SUM(CASE)`+HAVING (incremental == full `REFRESH`, both paths); dump/restore
(off+on) gains `mv_join_having` (auto-routed, restorable, correct after restore);
full suite + RR/SERIALIZABLE concurrency green.

---

## Earlier this work: deparse failing-group backfill → `SUM(CASE)` + HAVING

Enabled expression-arg aggregates with HAVING (single table) — e.g.
`SUM(CASE WHEN … END) … HAVING SUM(CASE …) > X` — by giving the HAVING
failing-group backfill a deparse builder.

- New `incr_build_backfill_sql_deparse`: deparses the view query over the REAL
  base tables (no ENR swap), with `havingQual` stripped and the
  `__mv_having_ok__` Const flipped to `false`, wrapped as
  `INSERT … <SELECT> ON CONFLICT (g) DO NOTHING` (+ `incr_emit_conflict_do_nothing`).
- Both the CREATE path and the **restore** path (`MatviewIncrPostRefresh`) now
  pick the backfill builder via the same `used_deparse` rule, so an
  expression-arg HAVING matview rebuilds identically on dump/restore. (This was
  caught by the dump/restore test: restore initially hit the hand backfill and
  failed on `CASE`.)
- `deparse_agg_shape` re-extended to allow single-table HAVING.

**Proof:** gold-standard oracle now **11 shapes** incl. `SUM(CASE)`+HAVING —
incremental == full `REFRESH`, both paths; dump/restore (off+on) gains
`mv_having_expr` (auto-routed, restorable, correct after restore, incl.
threshold crossings); full suite + concurrency green.

---

## Earlier this work: argument-aware HAVING-aggregate binding (latent bug fix)

Fixed a real correctness bug in HAVING maintenance (affects both the hand and
deparse paths): a HAVING aggregate was bound to a SELECT column by **function
OID only**, so `... SUM(a) sa, SUM(b) sb ... HAVING SUM(b) > X` filtered on
`sa` (the first sum) instead of `sb`.

- New `incr_having_agg_column(hagg, viewQuery)` binds a HAVING aggregate to its
  stored column by **full structural equality** (`equal()`), with `count(*)` →
  `__mv_count__`. Used by both `incr_validate_expr` (eligibility) and
  `incr_deparse_having_cond` (the `hav_sql` recompute), so the two can't
  disagree. HAVING aggregates absent from the SELECT list are rejected cleanly.
- **Deferred:** `SUM(CASE…)` + HAVING. It additionally needs a *deparse*
  failing-group backfill (`incr_build_backfill_sql_gen` is still a hand builder
  and can't render `CASE`). Tried and reverted within this increment; it's the
  next step.

**Proof:** gold-standard oracle extended to **10 shapes** incl. a two-same-OID-sum
HAVING shape that the old bug would have failed — incremental == full `REFRESH`
on both paths; full suite, dump/restore, concurrency green off+on; `SUM(CASE)`
+HAVING and HAVING-on-unselected-aggregate both rejected cleanly.

---

## Earlier this work: Phase 2 step 4 — single-table HAVING via the deparse core

Routed single-table HAVING aggregates through the deparse core (equivalence;
strangler consolidation), and added a `REFRESH`-oracle test that now covers
HAVING on both paths.

**What it does**
- `incr_build_delta_select_query` strips `havingQual` from the copy: the delta
  must compute per-group deltas for **every** group the transition rows touch
  (including groups that currently fail HAVING). HAVING itself is maintained
  separately by the `__mv_having_ok__` flag + the `hav_sql` recompute + the
  user-facing filtering view — all unchanged.
- Removed the `!hasHaving` gate from the **single-table** deparse branch (JOIN +
  HAVING still uses hand builders). Since expression-arg HAVING is not yet
  eligible, HAVING routes to deparse only when the GUC is on → a pure
  equivalence path, no behaviour change by default.

**Deferred (needs a prerequisite fix):** `SUM(CASE…)` + HAVING. The HAVING
recompute `incr_deparse_having_cond` resolves a HAVING aggregate to a SELECT
column **by function OID only**, so two sums with different arguments could bind
to the wrong column. That match must be made argument-aware before relaxing
eligibility for expression-arg HAVING — its own increment.

**Proof:** gold-standard oracle (`vs_full_refresh.sql`) extended to **9 shapes**
incl. two HAVING shapes — incremental == full `REFRESH` on hand AND deparse
paths; full suite, `having_teardown`, and dump/restore (incl. HAVING
threshold-cross-after-restore) green off+on; concurrency green.

---

## Earlier this work: Phase 2 step 3 — INNER JOIN via the deparse core

Extended the deparse delta core to **INNER JOIN aggregates** (the most common
Odoo report shape), and brought expression aggregate args along for free.

**What it does**
- ruleutils `get_rte_alias`: added the `RTE_NAMEDTUPLESTORE` case so the ENR
  FROM item carries its refname alias (`__mv_newtable s`). Without it a JOIN's
  qualified Vars (`s.amount`) couldn't resolve — this was the latent
  ENR-name-vs-refname issue flagged in the design.
- For each source table of a pure INNER JOIN, the delta swaps only that table's
  RTE for its transition ENR and leaves the others as relations, so ruleutils
  renders the join naturally. Routed to deparse when
  `dbblue_ivm_deparse_delta || incr_aggs_need_deparse(...)`.
- Eligibility relaxation (immutable expression args) extended from single-table
  to the pure INNER JOIN shape, via `incr_inner_join_deparse_shape` — mirrors the
  routing exactly, so a shape accepted at CREATE is rebuilt identically on restore.
- **Excluded (keep hand builders):** outer joins, self-joins, MIN/MAX, HAVING.

**Proof:** full suite off+on; dump/restore off+on now includes `mv_join` (plain)
and `mv_join_expr` (`SUM(CASE)` over JOIN) — both auto-routed, restorable, and
correct after restore under the **default GUC**; RR/SERIALIZABLE concurrency
green off+on. New test `phase2_join_deparse.sql` (5 PASS: equivalence on both
paths for both-table changes, `SUM(CASE)` over JOIN auto-routed + correct, and
OUTER/SELF-join expression args rejected).

---

## Earlier this work: Phase 2 step 2 — expression aggregate args (auto-routed)

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
  MIN/MAX via rescan under the matview-level serialization lock.
- Shapes: single-table aggregate, multi-table INNER/LEFT/RIGHT/FULL & CROSS
  JOIN, row-level (no GROUP BY), DISTINCT, HAVING, UNION ALL, WHERE filters,
  CTE/subquery normalization. All consistent with a full `REFRESH` under
  concurrent writers at every isolation level (recompute/multiset shapes
  serialize maintenance on a matview-level advisory lock; additive shapes are
  lock-free).
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
- Step 2: expression aggregate args (`SUM(CASE)`, `COALESCE`, immutable
  functions) auto-routed to deparse — maintainable and restorable.
- Step 3: INNER JOIN aggregates via deparse (incl. expression args over
  joins); ENR refname-alias fix in ruleutils.
- **Step 4: single-table HAVING via deparse (equivalence); gold-standard
  `REFRESH`-oracle test (`vs_full_refresh.sql`) covering 9 shapes, both paths.**

---

## What's left to do

### Next increment — widen the deparse gate shape-by-shape (strangler)
Migrate each shape to deparse and delete its hand builder once equivalent:
1. ✅ **INNER JOIN** (multi-table) — done (step 3). ENR refname-alias fixed.
   Outer/self joins still excluded.
2. ✅ **single-table HAVING** — done, incl. argument-aware aggregate binding and
   expression-arg HAVING (`SUM(CASE…)` + HAVING) via the deparse backfill.
3. ✅ **JOIN + HAVING** — done, incl. expression aggregates over the join.
4. **OUTER / SELF JOIN** — deparse the recompute/sync SELECT; subtle delta
   semantics (nullable side, both roles) — verify carefully.
5. **MIN/MAX** — keep the two-phase rescan; deparse only the scan SELECT.
6. **UNION ALL** — and revisit its concurrency certification.
- Each step: prove equivalence (suite + dump/restore + concurrency) with the
  GUC off and on, then remove the superseded hand builder.

### Remaining correctness fidelity
- Full NULL-group fidelity: maintain NULL group keys instead of excluding them
  (`NULLS NOT DISTINCT` unique index + `IS NOT DISTINCT FROM` joins), removing
  the documented divergence from a normal `REFRESH`.
- Exact all-NULL `SUM`: a per-column non-null counter so an all-NULL group
  shows SQL `NULL`, not `0` (current accepted residual; see `incr_nullsafe_accum`).

### Eventually
- ✅ Deparse is on by default (`dbblue_ivm_deparse_delta` boot_val = true);
  INNER JOIN is unconditional deparse, single-table defaults to deparse with the
  hand path kept only as a GUC-off escape hatch.  The one genuinely-unreachable
  hand call site (the non-self-join JOIN `else`) was removed; the hand builders
  themselves stay live (self-join's non-self tables + the escape hatch), so they
  are not deleted.  Fully retiring them / removing the GUC would also drop the
  A/B `REFRESH`-oracle safety net — left as an optional later step.
- ✅ READ COMMITTED safety for the recompute/multiset shapes (self-join, outer
  join, UNION ALL, row-level, MIN/MAX) via the matview-level serialization lock
  — see "Most recent work" above. Done with a single matview-level lock rather
  than per-affected-group locks: simpler, and the cost (serialized maintenance
  of *that* matview) is acceptable since these shapes are the heavyweight ones.
  Possible future refinement: per-group locks to let non-overlapping groups of
  one such matview maintain concurrently — only worth it if a real workload is
  bottlenecked on a single recompute-shape matview.
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
