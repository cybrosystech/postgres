# FK-Proven Outer-Join Reduction

Branch `feature/fk-outer-join-reduction` · worktree `/home/cybrosys/dbblue-19-base`
(off pristine `dbblue-19-base`, which remains identical to `upstream/REL_19_STABLE`)

| Commit | What |
|---|---|
| `97901331cec` | the reduction |
| `f787317f517` | three unsound cases found by adversarial audit |
| `7a556398c1d` | per-constraint trust catalog + verifier |
| `dc1ee79f415` | automatic withdrawal when enforcement is bypassed |
| `24c6653f038` | do not defeat PostgreSQL's own join removal |

---

## 1. What it does

An outer join cannot produce null-extended rows if every row of the preserved
side is guaranteed exactly one match on the nullable side. A `FOREIGN KEY`
supplies "at least one"; the unique constraint it references supplies "at most
one". Together they prove the outer join is an inner join.

PostgreSQL's `reduce_outer_joins()` previously reduced a join only when a strict
qual above it forced the nullable side non-null. It never consulted foreign
keys, `NOT NULL` constraints, or referenced-column uniqueness — even though all
three sit in the catalog.

**Why it matters.** Object-relational mappers emit `LEFT JOIN` for a
many-to-one reference unconditionally, because they cannot know per query that
the reference is mandatory. While the join stays outer, the referenced columns
may read NULL, so the planner cannot drive the scan from the referenced side's
index; an `ORDER BY ... LIMIT` over the join must then materialise and sort the
*entire* join output instead of stopping early.

Indexing does not fix this. The slow measurements below were taken **with the
ideal indexes already present**. Only the join reduction unlocks the plan.

---

## 2. Where it helps, and where it does not

Measured on a real Odoo 17 database (2093 MB, 923 tables, 3510 foreign keys).

### It helps

| Odoo screen | When | Off | On | Gain |
|---|---|---|---|---|
| Sales → Order Lines | **every load** | 493 ms | 1.10 ms | **449×** |
| Journal Items, sort by *Journal Entry* | on that click | 244 ms | 1.42 ms | **172×** |
| Purchase → Order Lines | every load | 28.9 ms | 1.12 ms | 26× |

### It does nothing

| Screen | Off | On | Gain |
|---|---|---|---|
| Inventory → Move Lines | 334 ms | 335 ms | 1× |
| Journal Items, sort by *Partner* | 322 ms | 328 ms | 1× |
| Order Lines, sort by *Customer* | 536 ms | 522 ms | 1× |
| Messages, sort by *Author* | 1933 ms | 1987 ms | 1× |
| Any full aggregate over the join | 1431 ms | 1435 ms | 1× |

### The rule that predicts it

Every win has a `NOT NULL` many2one; every non-win has a nullable one.

```
sale_order_line.order_id            NOT NULL = YES   -> 449x
account_move_line.move_id           NOT NULL = YES   -> 172x
purchase_order_line.order_id        NOT NULL = YES   ->  26x

account_move_line.partner_id        NOT NULL = no    ->   1x
sale_order_line.order_partner_id    NOT NULL = no    ->   1x
mail_message.author_id              NOT NULL = no    ->   1x
stock_move_line.result_package_id   NOT NULL = no    ->   1x
```

**It accelerates lists ordered through a *required* relation.** An optional
many2one genuinely can be NULL, so the `LEFT JOIN` really can null-extend and
reducing it would be *wrong*. The gate declining there is correct behaviour.

It also does nothing for full aggregates: there is no early termination to win
when the whole join must be read regardless. The gain comes from
`ORDER BY ... LIMIT` stopping early, plus join-order freedom.

### Why Odoo hits this at all

Odoo denormalises most sort keys onto the child table, so default list views
often need no join (`account.move.line._order = "date desc, move_name desc, id"`
— both stored locally). But `odoo/models.py:5259` adds a `LEFT JOIN` whenever it
orders by a many2one whose target model's `_order` is not literally `'id'`.
That happens in two situations:

1. **The model's own `_order` leads with a many2one** — joins on *every* list
   load. In this database: `sale.order.line` (`order_id, sequence, id`),
   `purchase.order.line`, `stock.move.line`.
2. **The user sorts by a relational column** — joins on that click.

Odoo's actual generated SQL, taken verbatim from the server log:

```sql
SELECT "sale_order_line"."id", ... 12 columns ...
FROM "sale_order_line"
LEFT JOIN "sale_order" AS "sale_order_line__order_id"
       ON ("sale_order_line"."order_id" = "sale_order_line__order_id"."id")
WHERE ("sale_order_line"."company_id" IN (1))
ORDER BY "sale_order_line__order_id"."date_order" DESC,
         "sale_order_line__order_id"."id" DESC,
         "sale_order_line"."sequence", "sale_order_line"."id"
LIMIT 80
```

Replayed: **500.4 ms → 1.17 ms (427×)**, byte-identical results.

### End to end, what a user waits for

The query is not the whole request. Measured over HTTP as an authenticated user:

```
feature OFF:  73 ms      (64 ms query + ~9 ms Odoo: Python, ORM, JSON, HTTP)
feature ON :   9 ms      (~1 ms query + ~9 ms Odoo)
                         = 8.1x, 64 ms saved
```

**The request cannot beat ~8× however fast the SQL gets** — Odoo's own ~9 ms
floor does not move. Do not expect the UI to become 400× faster.

Where it does become dramatic: a **cold cache** (the same query is 500 ms, not
64 ms, when `sale_order` is not in memory), **larger tables** (the avoided sort
is O(N)), and **concurrency** (64 ms of CPU and I/O removed per request per user
shows up as server headroom rather than one user's stopwatch).

---

## 3. Correctness

### The proof

`fkey_proves_inner_join()` in `src/backend/optimizer/prep/prepjointree.c`,
hooked into the `JOIN_LEFT` / `JOIN_RIGHT` cases of
`reduce_outer_joins_pass2()`. Every condition below is load-bearing:

- **Nullable side is a bare `RangeTblRef`** — a single ordinary relation scanned
  in full. Testing the relid set is not enough (see §4.2).
- **Partitioned tables must be expanding to partitions; plain tables must not
  be expanding to inheritance children.** `ONLY` on a partitioned parent reads
  no storage at all (§4.3).
- **No RLS policy or `TABLESAMPLE` on the nullable side** — either can hide the
  row the foreign key promises.
- **The ON clause is exactly the FK's column equalities**, using the FK's own
  `conpfeqop` operators, nothing more. Any extra qual, on either side, can
  reject the matching row and reintroduce null-extension.
- **Every referencing column is `NOT NULL`** — under `MATCH SIMPLE` a NULL
  satisfies the constraint with no referenced row present.
- **The referencing relation is not nullable from a lower outer join**, which
  would let those `NOT NULL` columns read as NULL.
- **The constraint is enforced, validated, and NOT deferrable.** A deferred
  constraint may legally be violated mid-transaction — exactly where the two
  join types disagree.
- **Not a constraint cloned to a partition** (`conparentid` set): a clone
  describes only its own partition and may reference a single leaf.
- **`MATCH SIMPLE` or `MATCH FULL` only.**
- **No upper qual forces a nullable-side Var to NULL** (§4.1).
- **Something outside the join condition uses the nullable side** (§4.4).

### The trust requirement

A foreign key is the only constraint class enforced by *triggers*. The catalog
can therefore record that enforcement was **armed**, never that the data
satisfies it. `session_replication_role = replica` (how a logical replication
subscriber applies changes), `ALTER TABLE ... DISABLE TRIGGER`, and a user
`BEFORE` trigger cancelling a cascaded delete all admit rows the constraint
forbids while leaving it validated and enforced.

PostgreSQL draws this line explicitly. On `NOT ENFORCED` constraints
(`create_table.sgml`): the system "might still assume that the data actually
satisfies the constraint for optimization decisions **where this does not affect
the correctness of the result**". That is why foreign keys already drive
selectivity estimation, where being wrong costs only time.

This optimization changes results, so it needs a stronger warrant — taken from
a new catalog, `pg_dbblue_trusted_fkey`. Same idea as `RELY` constraints
elsewhere, where an informational constraint may drive a rewrite only once an
administrator accepts responsibility for it.

---

## 4. Defects found, and how

Five real defects. The first three came from an adversarial audit; the last two
only appeared when measured against a real Odoo database. Each was reproduced
before being accepted.

### 4.1 The optimization validated its own foreign key — *critical*

`RI_Initial_Check()` validates a foreign key with exactly this shape:

```sql
SELECT ... FROM ONLY child fk LEFT OUTER JOIN ONLY parent pk
  ON (pk.id = fk.parent_id) WHERE pk.id IS NULL AND fk.parent_id IS NOT NULL
```

Reducing that collapsed the check to constant `false`. `ALTER TABLE ... ADD
FOREIGN KEY` then **accepted rows that violate the constraint** and marked it
validated, after which every `LEFT JOIN` over that key silently dropped the
orphans. The optimization manufactured the false premise it went on to rely on.

**Fix:** refuse to reduce whenever an upper qual forces a nullable-side Var to
NULL. Correct independently of validation — such a query asks for the rows that
did *not* match and must report what the data contains, not what the constraint
promises. It also stops the reduction pre-empting the existing
`JOIN_LEFT → JOIN_ANTI` conversion.

*Generalisable lesson: when an optimization reasons from a catalog fact, check
whether the query that establishes that fact is itself subject to the
optimization.*

### 4.2 Quals below the join were invisible — *critical*

The nullable side was identified by its relid set being a singleton. That is not
the same as "scanned in full": `pull_up_subqueries()` splices a pulled-up
subquery's whole `FromExpr` in as the join arm, **quals included**, and
`remove_useless_result_rtes()` does not run until after `reduce_outer_joins()`.

```sql
-- returned 1 row instead of 2
SELECT c.id, f.v FROM child c
LEFT JOIN (SELECT * FROM parent WHERE active) f ON c.pid = f.id
```

**Fix:** require the arm to be a bare `RangeTblRef`.

### 4.3 `LEFT JOIN ONLY <partitioned>` dropped every row — *major*

The `inh` guard exempted `RELKIND_PARTITIONED_TABLE` from the `inh` test
entirely, so `inh = false` — precisely what `ONLY` sets — passed. A partitioned
parent has no storage of its own, so every row should be null-extended;
reducing dropped the whole result (3 rows → 0).

**Fix:** the two relkinds have opposite requirements — partitioned **must** be
expanding, plain **must not** be.

### 4.4 It defeated PostgreSQL's own join removal — *major, found by real data*

`remove_useless_joins()` drops a `LEFT JOIN` whose inner side is unique on the
join key and otherwise unused — and the foreign key is what establishes that
uniqueness. So `SELECT count(*) FROM child LEFT JOIN parent ON ...` scans one
table and never joins.

Reducing it to an inner join forfeits the removal, because an inner join can
eliminate rows and so cannot be dropped. The plan went from a parallel scan to a
parallel hash join: **243 ms → 488 ms**.

This shape is not incidental — it is how Odoo counts rows for a list view's
pager, so **every list view paid the cost**, against a page query the reduction
was meanwhile making hundreds of times faster.

**Fix:** decline to reduce when nothing outside the join's own condition uses
the nullable side. Nothing to gain there anyway. Detection is deliberately
conservative: an unrecognised reference counts as no reference, which merely
plans the query the way PostgreSQL would have.

No synthetic benchmark caught this. An audit lens was explicitly tasked with
looking for it and did not find it.

### 4.5 Enforcement bypass — *inherent, mitigated*

See §3 and §5. Cannot be detected at plan time; addressed by the trust model.

---

## 5. The trust model

### Granting

```sql
SELECT * FROM dbblue_trust_foreign_keys();              -- whole database
SELECT * FROM dbblue_trust_foreign_keys('my_table');    -- one table
```

Scans the referencing table for violating rows and records trust only if it
finds none:

```
    relation     | constraint_name |  references  |       action       | violating_rows
-----------------+-----------------+--------------+--------------------+----------------
 sale_order_line | sol_order_fk    | sale_order   | trusted (verified) |              0
```

The check is phrased as `NOT EXISTS`, deliberately **not** the
`LEFT JOIN ... IS NULL` form `RI_Initial_Check()` uses, so its answer cannot be
influenced by the very optimization it is about to license.

On the real 2093 MB Odoo database: **all 3510 foreign keys verified clean in
5.2 seconds**, zero violations. That is the whole adoption cost.

A whole-database call also prunes declarations whose constraint has been
dropped. Trust rows store the constraint's relation and name beside its OID, so
a stale row cannot speak for a different constraint that later reused the OID.

### Withdrawing

```sql
SELECT * FROM dbblue_untrust_foreign_keys();            -- whole database
SELECT * FROM dbblue_untrust_foreign_keys('my_table');  -- one table
```

### Automatic withdrawal

A foreign key is enforced by its RI triggers and nothing else. When
`AfterTriggerSaveEvent` **skips** one — disabled, or because of the replication
role — that change went unchecked, and the declaration stops being warranted.
The constraint is noted and its trust row deleted when the transaction commits.

Doing it at commit matters in both directions:

| Scenario | Trust | Result |
|---|---|---|
| orphan written in replica mode, committed | **withdrawn** | rows visible again, no operator action |
| bypass via `DISABLE TRIGGER`, committed | **withdrawn** | rows visible again |
| bypass then `ROLLBACK` | **kept** | nothing was written |
| ordinary INSERT / UPDATE / DELETE | **kept** | no false revocation |

Cost is one lookup per constraint per bypass window — a constraint that is
already untrusted has nothing to withdraw — which keeps it off the replication
apply path after the first transaction.

The planner additionally refuses while the constraint's RI triggers are
disabled, and while the session is in replica mode.

### What this still does not cover

A declaration reflects the data at the moment it was made. Automatic withdrawal
covers bypasses **this server observes**. It cannot cover data that arrived
without those triggers ever being consulted — a restore that disabled them, or a
file-level table replacement. **Renew the declaration after any such
operation.**

---

## 6. Usage

### Enabling

```sql
SHOW dbblue_enable_fk_join_reduction;          -- master switch, default on
SELECT count(*) FROM pg_dbblue_trusted_fkey;   -- the effective state
```

Both must hold. The GUC being `on` with nothing trusted means **inactive** —
which is why an upgraded database sees byte-identical plans until someone opts
in. Read the second query, not the first, to know whether the feature is live.

```sql
-- session only; does NOT affect other connections such as an application's
SET dbblue_enable_fk_join_reduction = off;

-- database-wide; needs the application to reconnect
ALTER DATABASE mydb SET dbblue_enable_fk_join_reduction = off;
```

### Confirming a plan changed

```sql
EXPLAIN (COSTS OFF)
SELECT sol.id FROM sale_order_line sol
LEFT JOIN sale_order so ON sol.order_id = so.id
ORDER BY so.date_order DESC, so.id DESC LIMIT 80;
```

- inactive → `Hash Left Join` with a `Sort` above it
- active → `Nested Loop` + `Incremental Sort`, no full sort

### Watching it in the server log

```sql
ALTER DATABASE mydb SET log_min_duration_statement = 20;   -- then reconnect
```

With the feature inactive the join query appears; once trusted it drops below
the threshold and **disappears from the log entirely**. That vanishing is the
clearest evidence available.

---

## 7. Testing

`src/test/regress/sql/dbblue_fk_join_reduction.sql` — a separate file rather
than additions to `join.sql`, so upstream merges conflict only on one
`parallel_schedule` line. **246/246 tests pass.**

Coverage: the baseline reduction, the GUC, `RIGHT JOIN`, `FULL JOIN`, nullable
FK columns, absent FK, `NOT VALID`, `DEFERRABLE` (including violating one
mid-transaction), extra ON quals on both sides, wrong operator, lower outer join
nullability, composite keys, partial composite ON clauses, all four §4 defects,
automatic withdrawal (including that a rolled-back bypass keeps trust and
ordinary DML does not disturb it), and the join-removal guard.

**Two test-design traps hit while writing these**, both worth remembering:

1. A plan-shape classifier must recognise `Full` / `Anti` / `Semi` joins, or it
   reports them as "reduced" and the test passes for the wrong reason.
2. Several cases were **vacuous** until a column of the nullable side was
   selected, because `remove_useless_joins()` deleted the join entirely.

Always confirm a negative test can actually fail.

---

## 8. Honest limitations

- **Not a general speedup.** It accelerates `ORDER BY ... LIMIT` through a
  *required* relation. Full aggregates, optional relations, and unfiltered scans
  get nothing.
- **Scale-dependent.** The avoided sort is O(N); a 266k-row table gives 64 ms of
  saving, a million-row table proportionally more.
- **End-to-end gain is bounded by the application.** 427× on the query became
  8.1× on the HTTP request, because ~9 ms of Odoo overhead does not move.
- **Requires trust**, which requires integrity to actually hold. On a database
  where referential integrity is bypassed and not renewed, rows will be hidden.
- **The pager count is deliberately unaffected** — see §4.4. Reducing there was
  a regression, not a missed opportunity.
