# DBblue IVM — Overlay/Peel Decomposition Design

**Thesis:** many views are rejected only because of *projection-level* blockers —
a window function over the grouped output, a `now()`/`CURRENT_DATE` column, a
computed column — that do **not** affect the GROUP BY grain. Split the view into a
**maintained core matview** (grain keys + aggregates + immutable projections) plus
an **auto-generated read-time view** that re-adds the peeled expressions. The core
is a shape the existing engine already maintains byte-identically; the peeled
expressions are evaluated at read time from the core's stored columns.

> Research-ranked the #1 structural lever (see `project_ivm_coverage_strategy`).
> Basic variant is airtight for **+2 report views** (`hr_employee_skill_report`,
> `helpdesk_ticket_report_analysis`) byte-identically.

## The huge simplifier: reuse the HAVING infrastructure

HAVING already does exactly the "hidden base matview + user-facing view" split:
- `MatviewIncrSetup` renames the physical matview to `_dbblue_<oid>_base`
  (`RenameRelationInternal`, matview_incr.c ~1456).
- `incr_create_having_view` (matview_incr.c ~4189) creates `CREATE VIEW <origname>
  AS SELECT <cols> FROM <base> WHERE __mv_having_ok__` and records the INTERNAL
  base↔view dependency so DROP cascades both ways.
- `incr_link_having_base_to_view` re-wires that dependency on restore.
- createas.c restricts the base+view build to **WITH DATA** (a fresh WITH NO DATA
  HAVING matview warns and stays plain); dump/restore re-imports both objects.

Overlay/peel is the **same shape** with a richer view SELECT list. So the work is
NOT new lifecycle plumbing — it is (a) a peel analysis, (b) a core-query builder,
(c) a generalized view builder that renders peeled expressions over the base.

## Peel analysis (what is peelable)

An output `TargetEntry` is **peelable** iff removing it leaves the grain unchanged
and it can be reconstructed at read time from the core's stored columns:
- **Window function** (`WindowFunc`) whose window is over the grouped/row output
  (not an analytic window over pre-aggregation base rows). Byte-identical at read
  time (deterministic function of the core rows) — *except* `OVER ()` with no total
  ORDER BY, whose surrogate id is order-nondeterministic under REFRESH too (opt-in).
- **Non-immutable projection** — `now()`/`CURRENT_DATE`/STABLE/`concat` in an output
  column (`contain_mutable_functions`), the exact thing `incr_validate_projection`
  (matview_incr.c ~532) already rejects. Becomes read-time (a documented,
  arguably-fresher semantic — opt-in).

**Basic variant (airtight, ship first):** peel only expressions whose leaf refs are
**other output columns / group keys / stored aggregates** — no join-cardinality
change, no relocation of tables. Each leaf is already (or is added as) a core
column, so the overlay reconstructs the expression by binding its sub-expressions
to stored core columns — exactly `incr_having_expr_column`'s `equal()`-match trick.

**Hard guard (the sacred-invariant tripwire):** a volatile/wall-clock expression in
a **membership-determining position** (WHERE, any JOIN ON, GROUP BY, DISTINCT,
HAVING) is NOT peelable — it changes which rows exist with zero DML. Peel only
top-level SELECT-list expressions; reuse the whole-jointree volatility walk to
refuse otherwise. (Same class as the optional-side bug + now()-in-JOIN hole.)

## Pipeline

```
CREATE MATERIALIZED VIEW foo WITH (incremental_refresh=true) AS <select>
   │
   ▼  MatviewIncrIsEligible(core?)  ── if the ONLY blocker(s) are peelable projections
   │        AND the core (view minus peeled cols, plus any leaf cols they need) is eligible:
   ▼
  CORE matview  = <select minus peeled columns> (maintained by the existing engine)
   │  MatviewIncrSetup → rename to _dbblue_<oid>_base
   ▼
  OVERLAY view foo = SELECT <original output list, peeled exprs rebound to base cols> FROM _base
```

## Staged plan

**M-OV.1 — now()/volatile GROUP-BY projection (ship first).** Target
`helpdesk_ticket_report_analysis` (single table + LEFT JOIN rating + GROUP BY id +
avg; blockers `ticket_open_hours`, `sla_success` — both `now()` columns whose leaf
Vars `close_date`/`create_date`/`sla_deadline` are already output columns). No new
core columns needed. Steps:
1. `MatviewIncrPeelProjection(viewQuery, &core_query, &peeled_tes, &reason)` — detect
   peelable cols, verify core eligible, verify no volatile in a membership position.
2. createas.c: when `MatviewIncrIsEligible(vq)` fails only on projection immutability,
   try the peel; on success build the matview from `core_query`, stash `peeled_tes`.
3. Generalize `incr_create_having_view` → `incr_create_overlay_view(mvrelid, schema,
   name, viewQuery, peeled_tes)`: emit the original column order, each peeled expr
   rendered with sub-expressions rebound to base columns (reuse
   `incr_having_expr_column` + the `incr_deparse_having_cond` translator over stored
   cols), non-peeled cols as `base.<col>`, and (if HAVING) the `WHERE __mv_having_ok__`.
4. WITH-DATA-only + dump/restore: mirror HAVING exactly.
5. Test: `== REFRESH` of the ORIGINAL view (read-time now() differs by design — assert
   the stable columns byte-identical + the overlay column tracks read-time).

**M-OV.2 — window-over-output + surrogate id.** Target `hr_employee_skill_report`
(row-level over LEFT JOINs, `row_number()` id). Core is a row-level matview (Phase
9b); overlay adds `row_number() OVER (...)`. Surrogate id is opt-in
(order-nondeterministic under REFRESH too).

**M-OV.3 — extended peel (relocate grain-neutral PK LEFT JOINs).** Only behind a
conservative join-neutrality prover (unique/PK key, ≤1 match, feeds no aggregate).
Unlocks `report_project_task_user(_fsm)`, `hr_leave_report_calendar`. Higher risk.

## Open questions
1. Column-identity of the published view — the overlay must present columns in the
   original order/names/types; verify against the original view's tupdesc.
2. Read-time cost — the overlay re-evaluates peeled exprs per read; fine for
   now()/window, but document it.
3. Should the maintained core also be user-visible (as `_base`) or fully hidden?
   HAVING leaves `_base` reachable-but-hinted; mirror that.
4. Interaction with the cost router / REFRESH of the base (the overlay is a plain
   view, so `REFRESH MATERIALIZED VIEW foo` must target the base — mirror HAVING).

## Testing
Differential vs a full REFRESH of the ORIGINAL view: stable/aggregate columns must
be byte-identical; peeled now()/window columns are asserted equal to a read-time
recomputation (they are read-time by construction, not frozen).
