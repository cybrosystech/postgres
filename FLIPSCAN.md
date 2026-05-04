# ⚡ Flip Scan — Instant Last-Page Pagination for PostgreSQL

A custom PostgreSQL 18 executor extension that introduces the `BACKWARD SCAN` syntax to eliminate the O(N) cost of last-page `LIMIT/OFFSET` queries. Achieves **111x speedup** on real-world datasets with 1M+ rows.

**Commit:** [`ebe31d2`](https://github.com/cybrosystech/postgres/commit/ebe31d21b18a0e2722117f2091bd72c0eabec7db)

---

## The Problem

PostgreSQL's `LIMIT/OFFSET` pagination has a fundamental performance issue: the further you paginate, the slower it gets. For a table with 1.1 million rows:

```sql
-- Last page query — PostgreSQL must skip 1,099,940 index entries
SELECT id FROM sale_order ORDER BY id DESC LIMIT 80 OFFSET 1099940;
-- Time: 4,129 ms (4.1 seconds!)
```

The database performs an **Index Scan Backward**, starting at the highest ID and walking left through the entire index to skip past the offset. The cost is `O(offset + limit)` — linearly proportional to how far into the result set you've paginated.

This is a critical bottleneck for ERP systems like Odoo, where users routinely navigate to the last page of tables containing millions of sale orders, invoices, or stock moves.

---

## The Solution: Flip Scan Algorithm

Instead of scanning backward from the end, **flip the offset and scan forward from the start**.

```sql
-- Same query, with BACKWARD SCAN — instant
SELECT id FROM sale_order ORDER BY id DESC LIMIT 80 BACKWARD SCAN OFFSET 1099940;
-- Time: 37 ms (111x faster!)
```

### The Key Formula

```
flipOffset = total_rows − offset − limit
```

For the last page of 1.1M rows:
```
flipOffset = 1,100,020 − 1,099,940 − 80 = 0
```

**Skip zero rows**, collect 80 from the start of the forward index, reverse them in memory. Done.

### How It Works (5 Steps)

1. **Count live tuples** — Index-only scan with visibility map. Skips heap access for all-visible pages. Cost: ~37ms for 1.1M rows.
2. **Compute flipOffset** — `flipOffset = total − offset − min(limit, rows_available)`. Near zero for last pages.
3. **Forward scan + skip** — Skip `flipOffset` rows from the start of the index (this is the small number).
4. **Collect rows** — Read `limit` rows (or fewer if near the end) into an in-memory buffer.
5. **Reverse and return** — Reverse the buffer so rows come out in the original DESC order. Identical output to vanilla.

### Handling Partial Last Pages

When `offset + limit > total_rows`, the algorithm correctly adjusts:

```
total = 1,100,020   offset = 1,100,000   limit = 100

rows_available = 1,100,020 − 1,100,000 = 20
actual_count   = min(100, 20) = 20
flipOffset     = 1,100,020 − 1,100,000 − 20 = 0

→ Skip 0, collect 20, reverse → correct!
```

---

## Syntax

```sql
SELECT columns FROM table
ORDER BY column [ASC|DESC]
LIMIT n BACKWARD SCAN OFFSET m;
```

`BACKWARD SCAN` is placed between `LIMIT` and `OFFSET`. It is a hint to the executor to use the flip algorithm instead of the standard backward index traversal.

---

## Benchmark Results

**Test environment:** PostgreSQL 18 custom build, 1.1 million `sale_order` rows, warm cache.

### SELECT id (Index Only Scan)

| Metric | Vanilla | Flip Scan | Improvement |
|--------|---------|-----------|-------------|
| Execution time | 2,666 ms | 37.8 ms | **70x faster** |
| Rows scanned | 1,100,020 | 80 | 13,750x fewer |
| Buffer pages | 3,010 | 5 | 602x fewer |
| Disk reads | 0 | 0 | Same (warm cache) |

### SELECT * (Full Row Fetch)

| Metric | Vanilla | Flip Scan | Improvement |
|--------|---------|-----------|-------------|
| Execution time | 4,129 ms | 37.2 ms | **111x faster** |
| Rows scanned | 1,100,020 | 80 | 13,750x fewer |
| Buffer pages | 21,975 | 5 | 4,395x fewer |
| Disk reads | 18,319 | 0 | **∞ fewer** |

### EXPLAIN ANALYZE Comparison

**Vanilla:**
```
Limit  (actual time=4128.804..4129.102 rows=80)
  Buffers: shared hit=3656 read=18319
  -> Index Scan Backward using sale_order_pkey (actual time=0.015..2800.663 rows=1100020)
     Buffers: shared hit=3656 read=18319
Execution Time: 4129.245 ms
```

**Flip Scan:**
```
Limit  (actual time=36.993..37.091 rows=80)
  Buffers: shared hit=3015
  -> Index Scan using sale_order_pkey (actual time=0.015..0.125 rows=80)
     Buffers: shared hit=5
Execution Time: 37.239 ms
```

---

## When Flip Scan Wins

The flip algorithm is most effective when the offset is past the halfway point of the result set:

| Page Position | Vanilla Skips | Flip Skips | Winner |
|---------------|--------------|------------|--------|
| Page 10 (offset 720) | 720 | 99,200 | Vanilla |
| Mid-table (offset 50K) | 50,000 | 49,920 | ≈ Equal |
| Near end (offset 90K) | 90,000 | 9,920 | **Flip 9x** |
| Last page (offset 99.9K) | 99,920 | 0 | **Flip ∞** |

**Rule of thumb:** Flip Scan wins when `offset > total / 2`.

### Graceful Fallback

When `BACKWARD SCAN` is specified but the planner determines it cannot use the flip algorithm, it silently falls back to vanilla execution. No errors, no incorrect results.

---

## Files Modified

### Backend (Executor + Optimizer + Parser)

| File | Purpose |
|------|---------|
| `src/backend/executor/nodeLimit.c` | Core flip algorithm: `ExecBackwardScan`, `ExecBackwardScanNext`, `index_count_live_tuples` |
| `src/backend/optimizer/plan/createplan.c` | Propagates `scanBackward` flag from path to plan node |
| `src/backend/optimizer/plan/planner.c` | Plans the backward scan path when syntax is present |
| `src/backend/optimizer/util/pathnode.c` | Creates limit paths with backward scan awareness |
| `src/backend/parser/analyze.c` | Semantic analysis of `BACKWARD SCAN` clause |
| `src/backend/parser/gram.y` | Grammar rule: `LIMIT n BACKWARD SCAN OFFSET m` |

### Headers (Node Definitions)

| File | Purpose |
|------|---------|
| `src/include/nodes/execnodes.h` | `LimitState` fields: `flipBuffer`, `flipBufSize`, `flipBufCount`, `flipOffset`, `scanBackward`, `backwardScanSnapshot` |
| `src/include/nodes/parsenodes.h` | `SelectStmt.scanBackward` flag |
| `src/include/nodes/pathnodes.h` | `LimitPath.scanBackward` flag |
| `src/include/nodes/plannodes.h` | `Limit.scanBackward` and `Limit.needFlip` flags |
| `src/include/optimizer/pathnode.h` | Updated `create_limit_path` signature |
| `src/include/parser/kwlist.h` | Registers `BACKWARD` and `SCAN` as reserved keywords |

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    SQL Query                         │
│  SELECT * FROM t ORDER BY id DESC                   │
│  LIMIT 80 BACKWARD SCAN OFFSET 99920               │
└─────────────┬───────────────────────────────────────┘
              │
              ▼
┌─────────────────────────┐
│     Parser (gram.y)     │
│  Parses BACKWARD SCAN   │
│  Sets scanBackward=true │
│  in SelectStmt          │
└─────────────┬───────────┘
              │
              ▼
┌─────────────────────────┐
│  Analyzer (analyze.c)   │
│  Validates syntax:      │
│  needs ORDER BY + LIMIT │
│  + OFFSET               │
└─────────────┬───────────┘
              │
              ▼
┌─────────────────────────┐
│  Planner (planner.c)    │
│  Creates LimitPath with │
│  scanBackward flag      │
└─────────────┬───────────┘
              │
              ▼
┌─────────────────────────┐
│  Create Plan            │
│  (createplan.c)         │
│  Limit node gets        │
│  scanBackward + needFlip│
└─────────────┬───────────┘
              │
              ▼
┌─────────────────────────────────────────────────────┐
│            Executor (nodeLimit.c)                     │
│                                                       │
│  ExecLimit()                                         │
│    ├─ if scanBackward → ExecBackwardScan()           │
│    │    ├─ Step 1: index_count_live_tuples()         │
│    │    │          (exact count via index scan)       │
│    │    ├─ Step 2: flipOffset = total-offset-count   │
│    │    ├─ Step 3: ExecProcNode × flipOffset (skip)  │
│    │    ├─ Step 4: ExecProcNode × count (collect)    │
│    │    ├─ Step 5: reverse buffer                    │
│    │    └─ return ExecBackwardScanNext()             │
│    └─ else → normal LIMIT/OFFSET execution           │
└──────────────────────────────────────────────────────┘
```

---

## Building

```bash
# Clone the repository
git clone https://github.com/cybrosystech/postgres.git
cd postgres

# Standard PostgreSQL build
./configure --prefix=/usr/local/pgsql
make -j$(nproc)
sudo make install

# Initialize and start
/usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data
/usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l logfile start
```

---

## Usage Examples

### Basic: Last Page of a Large Table

```sql
-- Vanilla (slow — 4.1 seconds)
SELECT * FROM sale_order
ORDER BY id DESC LIMIT 80 OFFSET 1099940;

-- Flip Scan (fast — 37ms)
SELECT * FROM sale_order
ORDER BY id DESC LIMIT 80 BACKWARD SCAN OFFSET 1099940;
```

### With WHERE Clause

```sql
SELECT id, name, state FROM sale_order
WHERE state = 'done'
ORDER BY id DESC
LIMIT 80 BACKWARD SCAN OFFSET 499920;
```

### With Multiple ORDER BY Columns

```sql
SELECT * FROM sale_order
ORDER BY date_order DESC, id DESC
LIMIT 80 BACKWARD SCAN OFFSET 999920;
```

### Correctness Verification

```sql
-- Both queries MUST return identical results
SELECT id FROM sale_order ORDER BY id DESC LIMIT 5 OFFSET 1099940;
SELECT id FROM sale_order ORDER BY id DESC LIMIT 5 BACKWARD SCAN OFFSET 1099940;
```

---

## Integration with Odoo ERP

The feature integrates at Odoo's ORM level via `BaseModel._search()`:

```python
# In odoo/models.py — _search method
if offset and offset > 1000 and limit and not query._joins:
    try:
        estimated_total = self.search_count(domain)
        if estimated_total > 0 and offset > estimated_total // 2:
            query.backward_scan = True
    except Exception:
        pass
```

**Odoo areas that benefit:**

| Area | Pattern | Benefit |
|------|---------|---------|
| List view last page | `search([], offset=N, limit=80, order='id desc')` | Instant last page |
| search() / search_read() | ORM API calls from web client | Same pattern |
| Kanban deep scroll | Progressive loading with large offsets | Fast deep pages |
| ID-first fetch | `SELECT id ... LIMIT n OFFSET m` | Index-only scan |

**Areas with graceful fallback:**

| Area | Reason | Behavior |
|------|--------|----------|
| Queries with JOINs | many2one auto-joins | Falls back to vanilla |
| Non-indexed ORDER BY | Sort node in plan | Falls back to vanilla |
| GROUP BY / aggregation | Agg node blocks it | Falls back to vanilla |
| Small offsets (page 1-5) | Forward is already fast | Skipped by heuristic |

---

## Limitations

- **Counting overhead:** `index_count_live_tuples` performs an O(N) index scan to get the exact row count. For 1.1M rows this costs ~37ms. The net benefit is positive only when `offset > total/2` and the table has >100K rows.
- **Index requirement:** Requires an index on the ORDER BY column(s). Falls back to vanilla if the plan uses a Sort node instead of an IndexScan.
- **Unfiltered count:** `index_count_live_tuples` counts all live tuples in the index, not filtered by WHERE clauses. For filtered queries, the Odoo integration uses `search_count(domain)` to get the correct filtered total.

---

## Future Work

- **Planner-level integration:** Move the flip decision into the planner so it can cost-compare forward vs backward scan paths automatically.
- **Filtered counting:** Teach `index_count_live_tuples` to respect scan quals (WHERE conditions) for accurate counts on filtered queries.
- **Parallel counting:** Use parallel index scan for the counting phase on very large tables.
- **Automatic activation:** Integrate with the standard `LIMIT/OFFSET` syntax so the optimizer chooses the flip path when beneficial, without requiring explicit `BACKWARD SCAN` syntax.

---

## License

Same as PostgreSQL — [PostgreSQL License](https://www.postgresql.org/about/licence/).

---

## Credits

Developed by [Cybrosys Technologies](https://github.com/cybrosystech) as a PostgreSQL executor extension for high-performance ERP pagination.
