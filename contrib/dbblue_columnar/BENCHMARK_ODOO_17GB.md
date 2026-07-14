# Odoo-scale benchmark (17 GB) — honest results and the M6 gate

**Date:** 2026-07-14 · **Setup:** 17 GB Odoo-shaped DB (account_move_line 10 GB /
20M rows, ~480 B/row wide like real aml; sale_order_line 6M; stock_move 8M;
stock_valuation_layer 6M; account_partial_reconcile 8M), chronological dates
2021→2024 (realistic append order → tight date zone maps), realistic indexes,
14 GB RAM box, shared_buffers 1 GB, columnar store 4.6 GB resident (6 GB budget).
Columnar populate: full build ~56 s for all five tables (aml 36 s).

## Results (median of 3, planner free to choose, warm)

| report | columnar OFF | columnar ON | speedup |
|---|--:|--:|--:|
| R1 Trial Balance (as-of, grouped) | 3 525 ms | 4 969 ms | **0.71×** |
| R2 General Ledger (period detail) | 1 192 ms | 5 159 ms | **0.23×** |
| R3 Profit & Loss (period grouped) | 1 123 ms | 4 826 ms | **0.23×** |
| R4 Aged Receivable (partner aging) | 618 ms | 5 814 ms | **0.11×** |
| R5 Inventory Valuation (SVL) | 286 ms | 392 ms | **0.73×** |
| R6 Pivot company×account (2 yrs) | 2 837 ms | 5 620 ms | **0.50×** |

**The engine loses on every report at this scale.** This is the finding the
223 MB micro-benchmarks could not show, and it is why this benchmark was run
before calling anything v1.0.

## Root cause (verified with EXPLAIN (ANALYZE, BUFFERS))

1. **Serve-time validity reads the whole heap.** Every columnar query
   re-proves every block per query by reading all 32 heap pages
   (PD_ALL_VISIBLE + LSN == stamp) — R1 ON read 1.176 M buffers vs heap's
   1.047 M. On a 10 GB table that is a full-table IO pass per query.
2. **Zone-map skip fires after validity**, so it saves per-row CPU only —
   never IO. (Confirmed: R5 skipped 436/1722 blocks yet still lost.)
3. DSA chunk reads add memory traffic on top of the heap validity pass.
4. When the heap ≫ shared_buffers, parallel seq / index plans are IO-bound
   too — but they read the heap **once**, not heap + store.

The earlier 3–24× wins (BENCHMARK_M2.md) are real but were **warm
shared-buffers CPU wins** on a 223 MB table. At disk-resident scale the
validity pass dominates and inverts them.

## The gate to real wins: M6 — VM-fork-based block validity

Design (source-verified feasible on this tree):

> A block is valid iff **every covered VM bit is still ALL_VISIBLE** and the
> **VM page LSN equals the LSN stamped at build**.

Soundness: any modification of an all-visible page **clears its VM bit**
(caught by the bit check — the clear itself is the evidence; no LSN needed);
a later vacuum that re-sets the bit goes through `visibilitymap_set`, whose
callers **PageSetLSN the VM page** (pruneheap.c:2722, heapam_xlog.c:249) —
caught by the LSN check. False invalidation of neighbors under the same VM
page merely falls back to today's per-heap-page proof until the next refresh
re-stamps.

Effect: validity IO per query drops from the **whole heap (10 GB)** to the
**VM fork (~330 KB — 2 bits per heap page)**, ~30 000× less. Reports become
DSA-RAM/CPU-bound, where the engine's aggregate pushdown already delivers
multi-×. Zone-skip then also skips the heap IO, not just CPU. This is the
single prerequisite for the "22 s → 3 s" class of result on real hardware.

Caveats to resolve in M6 implementation: WAL-skip / empty-page
`visibilitymap_set` paths (heapam.c:2455) and build-time stamping order; both
have conservative fallbacks (treat as invalid → per-page proof).
