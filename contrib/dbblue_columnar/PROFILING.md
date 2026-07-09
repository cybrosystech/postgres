# DBblue Columnar Engine — Scan Profiling & the SIMD Decision

**Date:** 2026-07-08
**Build:** feature/columnar-engine @ 901f6d363d2 (post-M4)
**Question that triggered this:** "What about SIMD?"

## Decision

**DBblue does not add explicit SIMD to the scan/filter path now.**

This is an engineering decision backed by measurement, not a TODO. Future
contributors: do **not** add hand-written SIMD (AVX/SSE/NEON) intrinsics to the
predicate path on the assumption that "columnar engines need vectorized
comparisons." On this executor, scalar comparison speed is **not** the
bottleneck. Read the evidence below before revisiting.

## Why (one line)

Measurement showed the row-at-a-time executor overhead — per-row tuple emission
and a **redundant qual re-check** — dominates the scan, while the scalar
predicate compare that SIMD would accelerate is only ~11% of scan cost. So the
expected SIMD gain is a few percent, and even less on the selective queries
Odoo actually runs (they skip whole blocks before the filter ever runs).

## Method

`perf` and `gdb` are both unavailable in this environment
(`perf_event_paranoid=4`, `ptrace_scope` restricts attach), so profiling was
done by **controlled A/B experiment**: queries designed so that the difference
between two of them isolates one per-row cost. All runs are serial
(`max_parallel_workers_per_gather=0`), warm cache, 15 iterations via the
`dbbench()` harness, against `bench_aml` (2,000,000 rows, 891 columnar blocks,
9 registered columns). `EXPLAIN (ANALYZE)` confirmed **0 blocks skipped** for
the isolating queries, so the full 2M rows pass through the per-row path.

**Gotcha recorded for the next person:** the first attempt used out-of-range
constants (`company_id = 99`, `balance = -1e9`). Those got the *entire* relation
zone-map–skipped (every block's min/max excluded the constant), so they measured
"skip everything" (~4 ms), not the filter. To force per-row evaluation you must
use an **in-range** constant the zone map cannot exclude (here: `account_id`,
which has 500 distinct values, so `account_id = 250` touches every block).

## Measurements

| # | query | ms/exec | isolates |
|---|-------|--------:|----------|
| A0 | `count(*)` (metadata pushdown, no row walk) | 4.3 | block-directory walk baseline |
| A1 | `count(*) WHERE account_id = 250` (~4k survive, 0 skipped) | 22.9 | **pre-filter on 2M rows** (survivors negligible) |
| A2 | `count(*) WHERE account_id BETWEEN 1 AND 500` (all 2M pass) | 164 | pre-filter + emit + qual-recheck + agg on 2M |
| A3 | `sum(balance)` (no filter, emit 2M) | 141 | emit 2M + numeric sum (no qual) |
| A2b | `sum(balance) WHERE account_id BETWEEN 1 AND 500` (all pass) | 221 | A3 + pre-filter + qual-recheck on 2M |

### Attribution (per row, over 2M rows)

- **Pre-filter** (`dbbc_row_passes`, one `FunctionCall2Coll` btree compare per
  qual — *the thing SIMD would replace*):
  `A1 − A0 = 22.9 − 4.3 ≈ 18.6 ms / 2M ≈` **~9 ns/row (~11% of scan cost).**
- **Redundant qual re-check** (`ExecScan` re-applies `plan.qual`, which still
  contains the quals the pre-filter *already* evaluated exactly):
  `A2b − A3 − prefilter ≈ 221 − 141 − 18.6 ≈ 62 ms / 2M ≈` **~31 ns/row** —
  **3× the filter, and pure waste.**
- **Row emission + aggregate handoff** (`ExecClearTuple`, per-column fetch,
  `ExecStoreVirtualTuple`, advance the Agg): the remaining bulk of A2/A3,
  **~30–40 ns/row**.

## Reasoning

1. **SIMD target = ~11% of scan cost.** Best-case vectorization (~3×) of the
   filter saves ~6 ns/row → a few percent on full-pass filtered scans.
2. **Dominant cost is row-at-a-time emission + the redundant qual re-check**,
   ~7–8× the filter combined.
3. **Selective Odoo queries reduce the SIMD opportunity further.** Real
   reporting filters (recent date, one company) zone-map–skip most blocks, so
   the filter runs on a small fraction of rows — the part SIMD speeds up is
   already tiny in production.
4. **The pre-filter is selectivity-dependent, not universally good.** It *wins*
   when it filters rows out (avoids emitting them) but is *net overhead* when
   most rows pass (adds a compare that is then re-checked by `ExecScan`). This
   points at selectivity-awareness, not SIMD, as the near-term lever.

## Why the obvious quick fix (drop the redundant qual) was NOT taken now

Removing the pre-filter's quals from `plan.qual` to kill the ~31 ns/row
re-check collides with two hazards already hit in this codebase:

- **setrefs / rtoffset trap.** Quals carried in `custom_private` are opaque to
  `setrefs.c`, so their `Var` varnos never receive the rtoffset fixup — the
  exact bug that crashed COUNT-pushdown inside a CTE. Extraction works today
  *because* the quals live in `plan.qual` (which setrefs does fix). Moving them
  out breaks extraction unless the varno plumbing is redone.
- **Heap-fallback and whole-rel paths** rely on `plan.qual` to filter rows that
  bypass the pre-filter; removing quals from it silently returns wrong rows
  unless a slot-based filter is added to those paths too.

That is a delicate refactor of freshly-reviewed, correctness-critical code for
a **selectivity-dependent, moderate** win. Deferred deliberately.

## Future direction (in priority order)

1. **Selectivity-aware pre-filter** — skip the pre-filter (and its re-check
   waste) when a block/column zone map predicts most rows pass; keep it where it
   filters rows out. Cheap, no executor surgery.
2. **Batch / selection-vector execution** — process a column vector at a time,
   producing a selection vector instead of emitting one `TupleTableSlot` per
   row. This directly attacks the dominant cost (per-row emission + qual
   handling), not the 11% slice.
3. **Vectorized pipeline** — carry batches through filter → project → aggregate
   without materializing intermediate rows.
4. **SIMD as a natural consequence of (2)/(3)** — once data flows in column
   vectors, a vectorized compare falls out for free (and portably, via
   `-ftree-vectorize` on tight loops), with a scalar fallback for
   non-SIMD-able types (numeric, text, collated compares). SIMD is the *last*
   step inside a batch engine, not a bolt-on to the row-at-a-time path.

## Bottom line

The engine's demonstrated wins (2–24× in `BENCHMARK_M2.md`) come from **block
skipping + no tuple deforming + parallelism**, not from scalar-compare speed.
The next real performance milestone is **batch/vectorized execution**, inside
which SIMD becomes free. Adding SIMD before that would be optimizing the wrong
11%.
