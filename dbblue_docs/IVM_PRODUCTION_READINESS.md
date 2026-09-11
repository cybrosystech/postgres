# DBblue Incremental Materialized Views — Production Readiness

Branch: `feature/ivm-incremental-refresh` · HEAD `6c78ea39a96` · suite 38/38 green

## 1. The one guarantee

After **any** sequence of base-table DML, an incremental matview must be
**byte-identical to a full `REFRESH MATERIALIZED VIEW`** of the same definition.
Everything below serves that invariant. When a shape cannot meet it, the engine
**rejects it at CREATE** (`feature_not_supported`, 0A000) rather than silently
diverge — the "reject cleanly, never fail or diverge later" contract.

Engine: two strategies behind one gate (`MatviewIncrIsEligible`).
- **Additive** per-row delta (SUM/COUNT/AVG on single tables): commutative
  per-group arithmetic, unlocked, keeps per-group write concurrency.
- **Recompute-affected-groups** (outer/self joins, DISTINCT, MIN/MAX, multi-table
  aggregates, FD columns, stddev/variance): re-derives each touched group from the
  live tables under a **matview-level advisory lock + a fresh post-lock snapshot**,
  so the result equals a commit-time REFRESH at every isolation level.

## 2. Consistency model

`incremental == REFRESH at commit time` at **READ COMMITTED, REPEATABLE READ, and
SERIALIZABLE**. Recompute/multiset shapes take a matview advisory lock and read a
`GetLatestSnapshot()` captured *after* the lock (commit `81d072208be`), so a
maintainer serialized behind another sees the rows that one just committed. A
writer reading the matview **and** the base in one RR/SER transaction can "see the
future" (the maintenance read is outside its serializable snapshot); this is
deliberate and documented.

## 3. Supported shapes (maintain byte-identically)

- **Single-table aggregates**: `SUM`, `COUNT(*)`/`COUNT(col)`, `AVG`, `MIN`/`MAX`,
  `stddev`/`variance` family, `bool_and`/`bool_or`, `agg FILTER (WHERE …)`,
  `count(DISTINCT …)`.
- **`SELECT DISTINCT`** (all output columns) and `DISTINCT` self-joins.
- **Joins**: INNER, LEFT/RIGHT/FULL OUTER, and **aggregated** self-joins
  (incl. independent-duplicate), 2–5 tables. Multi-table additive aggregates take
  the matview lock (concurrency-correct).
- **GROUP BY**: plain columns, **IMMUTABLE** expressions
  (`date_trunc('month', <timestamp without tz>)`, `a+b`, …), and **bare
  functionally-dependent columns** (`SELECT id, name, sum(x) … GROUP BY id`, `id`
  a PK — proven via `check_functional_grouping`).
- **HAVING** (any supported aggregate/comparison), maintained via a hidden
  `__mv_having_ok__` flag + a read-time filtering view.
- **UNION ALL** (per-branch shapes, matching column types).
- **Overlay / read-time projection**: `now()`/`CURRENT_DATE`/STABLE/pure-surrogate
  (`row_number()`/`count(*) OVER ()`) in the **SELECT list** are split into a
  maintained core + a read-time view (byte-identical to a REFRESH at the same
  instant); combines with HAVING and FD columns.
- **STABLE GROUP BY keys** (`to_char`, `date_trunc` on timestamptz — the Odoo
  month-bucket pattern) **only with the explicit opt-in**
  `WITH (incremental_refresh=true, allow_stable_keys=true)` (see §5).

## 4. Rejected at CREATE (the whitelist boundary)

Cleanly declined (`feature_not_supported`) — no silent divergence, no crash:

- **STABLE group keys** without `allow_stable_keys` (§5).
- **Volatile/STABLE functions in a membership position** — WHERE / JOIN ON /
  HAVING (would drift with wall-clock time, zero DML; `270d8f8974c`).
- **Row-level (non-aggregated, non-DISTINCT) self-joins** — the self-matching
  diagonal pair has no correct two-arm delta (`6c78ea39a96`). *Aggregated/DISTINCT
  self-joins are supported.*
- **Partitioned source tables** — statement triggers with transition tables are
  not cloned to leaves, so direct-to-leaf DML would go unseen (`4cb27ddd9fe`).
- **GROUPING SETS / ROLLUP / CUBE** (`4cb27ddd9fe`).
- **Set-returning functions in SELECT**, **TABLESAMPLE**, **FOR UPDATE/SHARE**
  (`f9fa2379f68`).
- Window functions (except overlay-peelable pure surrogates), LIMIT/OFFSET, CTEs
  (inlined where possible), correlated/scalar subqueries (const-folded/normalized
  where possible), DISTINCT ON, non-UNION-ALL set ops, LATERAL, diamond joins
  (a table >2×), MIN/MAX over a self-join.

Expression positions (WHERE/HAVING/SELECT/agg args) run through
`incr_validate_expr`, itself a conservative **node-type allowlist** (explicit
permitted nodes, default reject).

## 5. Documented caveats (NOT defects)

These are cases where a full REFRESH is itself **not byte-deterministic**, or an
explicit opt-in:

- **Float `SUM`/`AVG`/`stddev`** — last-ULP differences from IEEE-754
  non-associativity; a plain REFRESH varies by scan order too.
- **Unordered `array_agg`/`string_agg`** — element order is unspecified in plain
  SQL (multiset-identical; add `ORDER BY` inside the agg for determinism, which is
  *rejected* today — a follow-up).
- **Numeric GROUP-BY-key display scale** for heterogeneous-scale keys
  (`3.0` vs `3.000`) — order-dependent in a plain REFRESH. (A numeric *measure*
  `sum` scale is the order-independent max and *is* maintained exactly.)
- **MIN/MAX numeric-tie** display scale — same class.
- **`allow_stable_keys` opt-in**: an untouched group can drift from REFRESH only
  after a session `TimeZone`/`lc_time` change; a REFRESH re-syncs. Accepted
  knowingly via the reloption.

## 6. Fixes this session (audit-driven)

Three parallel adversarial audit rounds + an inline round, each byte-identity vs
REFRESH on queries plain Postgres accepts:

| # | Defect | Commit |
|---|---|---|
| 1 | RR/SER recompute read frozen snapshot → lost update | `81d072208be` |
| 2 | FD bare GROUP BY columns errored on every delta (ENR lost PK) | `a137de56a9b` |
| 3 | `count(*) OVER ()` overlay star-drop; window-overlay + HAVING name collision | `eafd21587cc` |
| 4 | now()/STABLE in WHERE/JOIN/HAVING → silent time-drift divergence | `270d8f8974c` |
| 5 | Multi-table additive concurrency lost update (unlocked → matview lock) | `f8d3a1877ac` |
| 6 | MIN/MAX-over-join colliding group-key name → ambiguous GROUP BY delta error | `f8d3a1877ac` |
| 7 | Row-level self-join NULL-column stale delete | `f8d3a1877ac` |
| 8 | Unconstrained-numeric `sum` dscale drift on high-scale removal | `f8d3a1877ac` |
| 9 | Self-join join-key UPDATE on a both-sides node dropped rows | `4cb27ddd9fe` |
| 10 | Partitioned source silently stale (rejected) | `4cb27ddd9fe` |
| 11 | GROUPING SETS/ROLLUP/CUBE accept-then-fail (rejected) | `4cb27ddd9fe` |
| 12 | **SERVER CRASH**: no-aggregate GROUP BY missing `hasAggs` → parallel-agg SIGABRT | `6c78ea39a96` |
| 13 | Cost-router full-REFRESH ran before the fresh snapshot → RR/SER lost update | `6c78ea39a96` |
| 14 | Row-level self-join diagonal self-pair double-count (rejected) | `6c78ea39a96` |

Plus gate hardening: STABLE-key reject + `allow_stable_keys` opt-in (`b9e047d44a1`),
SRF/TABLESAMPLE/FOR-UPDATE reject (`f9fa2379f68`).

## 7. Audit convergence

| Round | Method | Genuine defects |
|---|---|---|
| 1 | 8 parallel adversarial agents | 4 |
| 2 | confirmatory re-audit, new angles | 3 |
| 3 | closure audit, deep-stress accepted shapes | 3 (incl. a server crash) |
| 4 | **inline** (serial): 13 angles incl. 300-step randomized + pgbench concurrency (RR) + dump/restore | **0** |

Honest read: the three **parallel** rounds each found a subtle tail through
independent adversarial exploration; the whitelist flip closed the *structural*
class, and the inline round (a weaker net than parallel agents) is clean — but a
clean inline round is **not** equivalent to a clean parallel round.

## 8. Operational notes

- **Dump/restore** verified: triggers/catalog/overlay-views/HAVING-base survive
  `pg_dump | restore` and keep maintaining == REFRESH.
- **Cost router** (`dbblue_ivm_refresh_threshold`, default `0.5`): a delta over
  50% of estimated live tuples falls back to a full REFRESH, now under a fresh
  snapshot + REFRESH's AccessExclusiveLock (concurrency-correct).
- **Never-crash contract**: the no-aggregate GROUP BY SIGABRT (#12) is fixed and
  regression-guarded under forced parallel aggregation.
- **Concurrency**: 6,429 pgbench transactions at REPEATABLE READ (12 clients,
  dimension-key + fact churn), 0 failed, matview == REFRESH == ground-truth base.

## 9. Recommendation

- **Controlled rollout** (vetted view definitions, monitored workloads):
  **ready.** The correctness core is strong; the two failure modes that could bite
  (silent divergence, accept-then-fail/crash) are exactly the classes closed this
  session, and the gate rejects the unsupported tail cleanly.
- **General availability** (arbitrary user-authored views): the whitelist gives a
  *defined* boundary, but the parallel-audit history shows the accepted breadth
  still surfaced a subtle tail across three rounds. Two viable paths:
  1. **Scope GA to the exhaustively-verified core** (single-table aggregates +
     simple inner-join roll-ups on the additive/recompute paths) and keep the
     long-tail shapes behind the flag.
  2. **Continue parallel adversarial auditing** until a full parallel round is
     clean, then GA at full breadth.

## 10. Open follow-ups

- Row-level self-join: proper delta (PK anti-join + delta⋈delta arm) to *support*
  instead of reject; likewise partitioned-source per-leaf maintenance
  (+ ATTACH/DETACH/row-movement).
- `array_agg`/`string_agg` **with** `ORDER BY` inside the agg is deterministic —
  accept it (currently rejected).
- Cost router: unit-test the concurrent large-delta path directly.
- Coverage levers deferred in `project_ivm_coverage_strategy` (M-OV.3 join
  relocation, 34-col key handling, row-level overlay, correlated subqueries).
