# DBblue IVM — Phase 2: Query-tree Delta Core (Design)

Phase 2 replaces the ~18 hand-written per-shape SQL "string builders" (the delta
generator in `matview_incr.c`, ~80 hand-deparse sites) with a principled
**Query-tree + `ruleutils` deparse** core, so arbitrary expressions
(`CASE`/`COALESCE`/`FILTER`), column aliasing, and NULL semantics are handled by
PostgreSQL itself. This is the design and the incremental migration plan.

Source: produced and adversarially verified by the `ivm-phase2-design` workflow
(3 competing designs, judged, 8-case adversarial verification), grounded in
verified source line numbers.

---

## Chosen approach: RULEUTILS-DEPARSE delta core

> Copy the matview's stored `Query`, swap the changed source table's
> `RTE_RELATION` → `RTE_NAMEDTUPLESTORE` (the transition-table ENR), deparse
> with an ENR-aware `get_query_def`, and feed the text through the **existing**
> catalog → SPI plan-cache → `ON CONFLICT` apply. The catalog, trigger,
> two-phase MIN/MAX lock, dump/restore, and counting algorithm are unchanged.

Chosen over two alternatives: *precompute-at-setup* (near-twin, but its only
differentiator was a brittle textual ENR substitution to avoid the one ruleutils
edit — wrong trade) and *direct-Query-execution* (genuinely sidesteps deparse,
but highest regression surface: hand-built `SPIPlan`, perminfo pruning, a new
catalog column, and an O(rows) per-row upsert — a likely perf regression).

### Why it wins
- **Changes the least.** Only the INS/DEL *SELECT body* is produced by deparse;
  the INSERT/`ON CONFLICT`/accumulation wrapper, CLN/HAV/LOCK builders, plan
  cache, trigger, and apply are reused byte-for-byte.
- **Cannot drift.** The deparsed text is re-parsed by the *same* SPI path
  production already uses (the bare ENR name binds via `name_matches_visible_ENR`
  against the trigger's query environment).
- **Same catalog & dump/restore shape** — still 5 text columns; no migration.

---

## Feasibility: PROVEN

The one required core change (committed, `ea92a4e5ea9`): an additive
`RTE_NAMEDTUPLESTORE` case in `get_from_clause_item` (mirrors `RTE_CTE`) + a thin
extern `dbblue_deparse_query()` over the static `get_query_def`. With it, a real
plain-aggregate delta query deparses correctly:

```sql
-- view: SELECT g AS k, SUM(amt) s, COUNT(*) c, AVG(amt) a, SUM(amt*qty) sx
--       FROM p2t WHERE amt>0 GROUP BY g
-- deparsed INSERT-side delta SELECT:
SELECT g AS k, sum(amt) AS s, count(*) AS c, avg(amt) AS a,
       sum((amt * (qty)::numeric)) AS sx,
       sum(amt) AS __mv_avgsum_a, count(amt) AS __mv_avgcnt_a, count(*) AS __mv_count__
FROM __mv_newtable WHERE (amt > (0)::numeric) GROUP BY g
```

Note: the ENR (`__mv_newtable`) is named in FROM; the output alias `g AS k`
renders correctly (no Bug-A class); the hidden counting columns are present; and
`SUM(amt*qty)` — an aggregate-arg expression the hand builders can't emit —
renders natively. That is the entire Phase-2 thesis, demonstrated.

---

## Delta-Query construction (plain single-table aggregate)

`incr_build_delta_select_query(viewQuery, srctable, enrName)`:
1. `copyObject` the stored view query (it already carries the hidden targets
   from `MatviewIncrAddCountTarget`, so the deparsed SELECT yields the matview's
   full column set).
2. Find the one `RTE_RELATION` with `relid == srctable`; convert it *in place*:
   `rtekind = RTE_NAMEDTUPLESTORE`, `enrname = __mv_newtable`/`__mv_oldtable`,
   keep `relid` (plan invalidation) and `eref` (column names the ENR mirrors
   1:1), build `coltypes/coltypmods/colcollations` from the source tupdesc
   (zeros for dropped cols, exactly as `addRangeTableEntryForENR`), clear
   `perminfoindex` (ENRs need no permission check; deparse ignores it anyway).
3. No synthetic WHERE: scanning only the transition tuplestore + the existing
   GROUP BY already produces the per-group deltas. (A `key IN (SELECT … FROM enr)`
   qual is added later only for HAVING/MIN-MAX backfill — ordinary deparsable SQL.)

Two copies per (mv, src): `__mv_newtable` (insert delta) and `__mv_oldtable`
(delete delta). The deparsed SELECT is wrapped textually by the existing shells.

---

## Apply / merge — unchanged
- **INS**: `INSERT INTO mv (cols) <deparsed SELECT> ON CONFLICT (gcols) DO UPDATE
  SET <accum>` — the `incr_nullsafe_accum` / AVG-recompute block is reused.
- **DEL**: `WITH d AS (<deparsed SELECT>) UPDATE mv SET <subtract> FROM d WHERE
  <key join>` — reused.
- **CLN/HAV/LOCK**: unchanged (don't reference the delta SELECT body).
- Per-aggregate strategy: deparse owns expression/aggregate rendering + NULL
  semantics; the wrapper owns cross-row accumulation. AVG stays (sum,count);
  MIN/MAX keeps its two-phase lock + rescan.

---

## Adversarial verdicts (what holds / what to watch)

| Case | Verdict |
|---|---|
| Column aliasing (`g AS k`, Bug-A class) | ✅ holds — and never enters the path: MIN/MAX views bypass deparse entirely in step 1 |
| AVG over NULLs (incl. all-NULL group) | ✅ holds (the `(sum,count)` pair makes it exact) |
| Standalone `SUM(x)` all-NULL group | ⚠️ inherits Phase-1 residual (0 vs SQL-NULL); fix = per-column non-null counter (roadmap) |
| NULL **group key** | ✅ holds — inherits the auto-exclude `IS NOT NULL` filter from the stored query (consistent with REFRESH) |
| MIN/MAX two-phase delete | ⚠️ **must bypass the deparse core entirely (INS *and* DEL) in step 1** — gate on `incr_has_minmax_agg`; keep its builders verbatim |
| Concurrency (plan cache, snapshots) | ✅ holds — machinery byte-identical |
| dump/restore re-arm | ✅ holds — same 5 text columns; re-armed at CREATE→setup→re-deparse |
| `get_query_def` on an ENR RTE | ✅ requires the one additive case (done); proven |

**Latent (JOIN phase only, not step 1):** deparsed `Var`s qualify with the RTE's
*refname* (from `rtable_names`), which may diverge from `enrname`. For a single
table no qualification is emitted, so step 1 is safe; the JOIN phase must ensure
the emitted relation-reference name and the Var-qualifier name agree.

---

## Strangler migration (incremental, regression-gated)
1. Add GUC `dbblue.ivm_deparse_delta` (default off) + a shape gate.
2. Factor the INS head + `ON CONFLICT`/accum tail and the DEL CTE/UPDATE shell
   out of the existing builders into `incr_wrap_ins_shell` / `incr_wrap_del_shell`
   (have the OLD path call them too → free no-op regression checkpoint).
3. New `incr_build_{ins,del}_select_deparse` = `incr_build_delta_select_query`
   → `dbblue_deparse_query` → wrap. Gate to `PLAIN_SINGLE_TABLE_AGG` &&
   `!incr_has_minmax_agg`.
4. Prove equivalence: run the whole `src/test/dbblue_ivm/` suite with the GUC
   **off** and **on**; the `_cmp()` live-recompute assertions must report 0 diffs
   in both. Add one positive test only the deparse path can pass:
   `SELECT g, SUM(CASE WHEN amt>0 THEN amt ELSE 0 END), COUNT(*) … GROUP BY g`.
5. Widen the gate shape-by-shape: JOIN → (then) MIN/MAX → HAVING → UNION, deleting
   each old builder as its deparse equivalent passes. Old builders stay
   compilable until each shape is migrated.

**Unlocks** (once expressions render via deparse): `SUM(CASE…)`, `COALESCE`,
scalar expressions, `GROUP BY <expr>`, `FILTER`, and full NULL-group fidelity.

---

## Status
- ✅ Foundation committed (`ea92a4e5ea9`): ENR-aware `get_query_def` +
  `dbblue_deparse_query`; verified non-regressing; feasibility proven.
- ✅ **Step 1 landed**: GUC `dbblue_ivm_deparse_delta` (default off,
  `DEVELOPER_OPTIONS`); `incr_build_delta_select_query` (copy view Query, swap
  source RTE → transition-table ENR); INS/DEL shells factored out of the hand
  builders so both paths share the merge logic byte-for-byte; deparse builders
  wired for the **plain single-table aggregate** shape only (MIN/MAX and HAVING
  bypass — their delta SELECT must not be a literal render of the view query).
  - Equivalence proven: full `dbblue_ivm` suite passes with the GUC **off and
    on**; dump/restore passes off and on (the deparse-generated catalog SQL
    round-trips and re-arms); RR/SERIALIZABLE concurrency passes off and on.
  - Correctness bonus: fixed a latent **default-path** bug — the hand deparser
    treated every single-arg `FuncExpr` as a cast, silently dropping
    `floor()`/`abs()` and corrupting the running total. Now rendered as a call.
  - New test: `src/test/dbblue_ivm/phase2_deparse_delta.sql`.
- ⏭ Next increment: **auto-routing** for shapes only deparse can express
  (`SUM(CASE…)`, `COALESCE`, scalar/aggregate-arg expressions) — the engine
  must select deparse automatically (GUC-independent) so such matviews remain
  restorable; eligibility relaxation scoped to plain single-table aggregates;
  plus a dump/restore test for the new shapes. Then widen shape-by-shape:
  JOIN → MIN/MAX → HAVING → UNION, deleting each hand builder as it lands.
