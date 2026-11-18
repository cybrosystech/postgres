# DBblue Columnar Engine — Milestone 2 benchmark (Odoo-shaped)

Measures the engine after M2 (store + serve + predicate pushdown + COUNT/MIN/MAX
pushdown) against plain heap, to let numbers pick the next milestone.

## Setup
- `bench_aml`: `account_move_line`-shaped, **2,000,000 rows**, 223 MB heap (fits
  in `shared_buffers = 512MB` → fair CPU-vs-CPU comparison, not an I/O artifact).
- `date` is **chronological / correlated with insert order** (as real accounting
  entries are) → zone maps are tight, which is realistic, not cherry-picked.
- 5 companies, ~90% `posted`, 500 accounts, 20k partners. Columnar store: 891
  blocks, 200 MB, fully populated.
- Each query timed 10×, warm cache, columnar ON vs OFF. **Correctness: 0
  mismatches** (columnar result == heap for every query).
- Two regimes: **serial** (heap serial) and **parallel** (heap `Parallel Seq
  Scan`, 4 workers; the columnar path is serial — it has no parallel mode yet).

## Results (ms/exec; ×= heap/columnar, >1 means columnar faster)

| Query | ser col | ser heap | ser × | par col | par heap | par × | columnar chosen (par)? |
|---|--:|--:|--:|--:|--:|--:|:--|
| Q1 `count(*)` | 20.1 | 63.7 | **3.2** | 3.8 | 23.4 | **6.2** | yes (metadata) |
| Q2 `min/max(date)` | 4.9 | 116.1 | **23.9** | 4.6 | 39.8 | **8.7** | yes (metadata) |
| Q3 `sum` recent 6mo | 38.2 | 127.8 | **3.4** | 38.2 | 42.2 | 1.10 | yes (zone-skip) |
| Q4 `sum` company+date | 26.9 | 110.7 | **4.1** | 37.5 | 39.7 | 1.06 | yes |
| Q5 `sum` IN-list+state | 112.0 | 196.2 | 1.75 | 61.3 | 64.8 | 1.06 | ~tie |
| Q6 `count` selective 1mo | 10.7 | 97.0 | **9.0** | 8.3 | 35.9 | **4.3** | yes (zone-skip) |
| Q7 drill-down scan | 16.9 | 101.9 | **6.0** | 18.7 | 38.0 | **2.0** | yes |
| Q8 `GROUP BY` company | 97.9 | 194.6 | 2.0 | 63.4 | 61.9 | 0.98 | **no → parallel heap** |
| Q9 `GROUP BY` account top10 | 54.9 | 144.9 | 2.6 | 55.5 | 49.5 | 0.89 | **no → parallel heap** |
| Q10 full `sum` (unselective) | 124.2 | 212.4 | 1.7 | 65.2 | 66.7 | 1.02 | **no → parallel heap** |

## What the numbers say

**Validated decisively (win even vs parallel heap):**
- **Metadata pushdown** (COUNT/MIN/MAX): Q1 6.2×, Q2 8.7×, Q6 4.3× — reads ~no
  data, so parallelism can't catch it. `min/max(date)` at 23.9× serial is the
  headline.
- **Zone-map skipping** on the correlated `date` column: Q6 9×/4.3×, Q7 6×/2×,
  Q3 3.4× — the more selective the filter, the bigger and more parallel-proof
  the win. This is the core Odoo dashboard pattern (filter by recent date).

**The gap the numbers expose:**
- Columnar has a real **~1.7–4× per-tuple CPU advantage** on scan-heavy
  aggregates (proven by the *serial* column: Q3 3.4×, Q4 4.1×, Q8 2.0×, Q10
  1.7×) — no heap deform + skip.
- But that advantage is **serial**, and against 4-way parallel heap it
  evaporates: Q3/Q4/Q5 win by only ~5–10%, and for Q8/Q9/Q10 the planner
  **correctly falls back to parallel heap** (columnar not chosen) because its
  serial cost exceeds parallel heap's.
- So on the bread-and-butter reporting queries (filtered `SUM`, `GROUP BY`),
  columnar's win is **left on the table** purely because it can't parallelize.

## Recommendation for the next milestone

**Parallel-aware columnar scan.** Make `DBBlueColumnarScan` a parallel CustomScan:
partition the block directory across workers via a shared atomic cursor in DSM;
each worker runs the same validity + zone-skip + no-deform emit over its block
slice; results combine through the existing `Gather` / Parallel Agg. Expected
effect: columnar keeps its 2–4× per-tuple edge **and** parallelizes, so it beats
parallel heap by that factor and the planner chooses it — lifting the entire
filtered-`SUM` and `GROUP BY` class at once (Q3, Q4, Q5, Q8, Q9, Q10), not one
shape.

This is higher-leverage than GROUP BY pushdown: Q8/Q9 lose to parallel heap
because of the parallelism gap, not the lack of GROUP BY pushdown — a parallel
columnar scan feeding the normal parallel Agg already flips them. GROUP BY
pushdown is a good follow-on, on top of a parallel scan.
