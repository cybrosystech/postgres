# Skip-If-Unchanged for REFRESH MATERIALIZED VIEW

Branch `feature/matview-skip-unchanged-fixes` · worktree `/home/cybrosys/dbblue-matview-skip`
(off `feature/matview-skip-unchanged`, with pristine `dbblue-19-base` merged in first)

| Commit | What |
|---|---|
| `9af1922caf2` | the original feature: `auto_skip_unchanged` reloption + shmem dirty flags |
| `1516bda21b5` | xact callback cleared its list before COMMIT could flush it |
| `2572e710d61` | merge `dbblue-19-base` (== `upstream/REL_19_STABLE`) |
| `2f44e960b02` | rewrite: eleven silent-staleness defects, or refuse to skip |

---

## 1. What it does

A materialized view defined `WITH (auto_skip_unchanged=true)` answers
`REFRESH MATERIALIZED VIEW` without rebuilding, whenever it can prove that
nothing the view depends on has changed since its own last successful refresh.

```sql
CREATE MATERIALIZED VIEW daily_sales WITH (auto_skip_unchanged=true) AS
    SELECT partner_id, sum(price_total) FROM account_move_line GROUP BY 1;
```

A skipped refresh costs the same regardless of how large the sources are:

| | 2M-row `count(DISTINCT v)` |
|---|---|
| rebuild (sources changed) | 482 ms |
| skipped (sources unchanged) | **0.62 ms** |

**Why it matters.** Scheduled refresh jobs are usually sized for the worst case
and then run on a fixed interval, so most executions rebuild a view whose
sources nobody touched. The cost of that is proportional to the source tables,
not to the amount of change, and it is paid every time.

**What it is not.** It does not make `REFRESH` cheaper when the sources *have*
changed, and it does not make `REFRESH` non-blocking — see §6.

---

## 2. Where it helps, and where it does not

### It helps

A view whose sources are written much less often than the view is refreshed.
The clearest case is a reporting view over closed periods: last month's ledger
does not change, but a nightly job rebuilds the view anyway.

### It does nothing

| Situation | Why |
|---|---|
| Sources written continuously | Every refresh finds a changed source. Same shape as the offset-flip count cache: a source taking sustained writes never presents a clean window. |
| The defining query calls `now()` | Non-`IMMUTABLE` functions are rejected (§4). This rules out "as of today" reporting views, which is the single largest coverage loss. |
| `REFRESH ... CONCURRENTLY` | Excluded; no commit witness exists on that path (§3). |
| Row-level security on any source | A policy change alters contents with no write, so it cannot be tracked. |
| Sources that are sequences, foreign tables or system catalogs | Their contents change without a heap write. |
| `REPEATABLE READ` / `SERIALIZABLE` | The data-fill snapshot is pinned and may predate the observation (§3). |

### The rule that predicts it

> The view is skippable if it is a pure function of ordinary tables, and those
> tables were quiet since its last refresh.

Everything in the "does nothing" table is one of those two clauses failing.
Views themselves are fine — they are expanded to their base tables, and
redefining one forces a rebuild.

---

## 3. Correctness

A wrong "nothing changed" answer is the only failure this feature can have, and
it is a bad one: `REFRESH` reports success and the view silently serves stale
data. Every rule is therefore written to fail toward rebuilding.

### The state

- **Per source table**, a monotone counter `gen`, only ever incremented, bumped
  at `XACT_EVENT_COMMIT` — which `CommitTransaction` reaches *after*
  `ProcArrayEndTransaction`, so a bump always strictly follows the moment the
  writer's rows became visible. Also bumped at `XACT_EVENT_PREPARE`, which no
  later callback follows.
- **Per materialized view**, a watermark: the `(relid, gen, relfilenumber)`
  vector observed at that view's own last successful refresh, plus the view's
  own post-swap `relfilenumber` and a fingerprint of its rewritten query.

Keying the watermark **per view** rather than per source is not an optimisation;
it is the fix for §4.10.

### Why skipping is sound

Before reading a source's counter, `ConditionalLockRelationOid(relid,
ShareLock)` must succeed, and is released immediately. `ShareLock` conflicts
with `RowExclusiveLock`, which every writer holds from its first write until
*after* its commit callbacks have run, and which a prepared transaction's dummy
`PGPROC` holds until `COMMIT PREPARED`. So acquiring it proves no writer to that
source is in flight, prepared, or mid-commit — therefore every write that is
visible has already been counted. Acquiring it also forces
`AcceptInvalidationMessages()`, so the `relfilenumber` read afterwards is not a
stale relcache value.

This one test is what makes a separate in-flight counter, a
`twophase_rmgr` registration for 2PC, and a separate relcache guard all
unnecessary.

### Why recording a watermark is sound

The counters are captured *before* the data-fill, and the data-fill then runs on
a freshly pushed `GetLatestSnapshot()`. Any writer whose rows are absent from
that snapshot becomes visible after it, hence after the capture, hence bumps the
counter after the capture, hence leaves the watermark not matching. A writer
that commits between capture and snapshot costs one extra rebuild, which is the
safe direction.

### Why an aborted refresh is safe

The watermark stores the `relfilenumber` the view received from its heap swap.
If the transaction rolls back, `pg_class` reverts and the view keeps its old
`relfilenumber`, which no longer matches — so the view reads as changed from
then on. That makes rollback, subtransaction abort, and crash safe with **no
commit-time callback at all**, and it is why `CONCURRENTLY` is excluded:
`refresh_by_match_merge` updates in place, so no such witness exists.

---

## 4. Defects found, and how

Eleven distinct defects, every one of which left the view **silently stale**
with `REFRESH` reporting success. Nine were reproduced as failing tests against
a running server before being fixed; the two marked *structural* were
established from the stored rule and the catalog rather than from a live stale
result. All eleven have a regression test now.

| # | Defect | How found |
|---|---|---|
| 4.1 | Sources in a FROM-subquery, CTE or SubLink were never enumerated — and an empty source set fell through to "clean", so the view never refreshed again *at all* | reproduced |
| 4.2 | A source **view** was enumerated as itself, and so could never be seen to change | *structural* |
| 4.3 | `TRUNCATE`, and any other rewrite, bypassed the write hooks | reproduced |
| 4.4 | Partitioned sources tracked only the parent, but writes land on leaves; `ATTACH` / `DETACH` / `DROP` of a partition changed contents with no write at all | reproduced |
| 4.5 | A rolled-back `REFRESH` still cleared the flags, stranding the view | reproduced |
| 4.6 | `WITH NO DATA` then plain `REFRESH` skipped, leaving the view permanently unpopulated and unreadable | reproduced |
| 4.7 | A `REFRESH` in the same transaction as a write to its own source skipped | reproduced |
| 4.8 | Refreshing a source materialized view left its dependents stale | reproduced |
| 4.9 | A row committing *during* the refresh window was lost for good | reproduced |
| 4.10 | Cleanliness was per source, not per (view, source) | reproduced |
| 4.11 | A transaction touching more relations than the local array held dropped the overflow silently | *structural* |

Separately, and not counted above, the rewrite added the reject list in §2 —
sequences, system catalogs, `TABLESAMPLE`, virtual generated columns,
non-`IMMUTABLE` functions and row-level security. Each of those changes what the
view should contain with no tracked write, so each was a hole of the same class;
they are hardening rather than defects in the original design, and each has a
test (`T16`, `T19`–`T23`).

### 4.2 A source view could never be seen to change — *critical*

The stored `_RETURN` rule query is **parse-analyzed but not rewritten**. A source
view sits in it as an `RTE_RELATION` with `relkind='v'`, and its base tables
appear nowhere:

```
:rtable ({RANGETBLENTRY ... :rtekind 0 :relid 16412 :inh true :relkind v ...})
```

A view has no heap, so it never received a write and its `relfilenumber` never
moved — the view was clean forever. The comment above `dataQuery` in
`matview.c` asserts the opposite ("was rewritten at the time of the MV
definition") and is misleading; `refresh_matview_datafill` rewrites per refresh.
Enumeration now does the same `copyObject` → `AcquireRewriteLocks` →
`QueryRewrite` before walking, which is also what makes view *redefinition*
detectable, via the query fingerprint.

### 4.9 A row committing during the refresh was lost — *critical*

A single `bool` cannot distinguish "dirty before my scan" from "dirty during my
scan", so mark-clean clobbered the second. Reproduced with a deliberately slow
refresh and a write committed two seconds into it: the view stayed at 11 rows
against a truth of 12, and no number of further `REFRESH`es ever recovered it.
Fixed by the counter plus capture-before-snapshot ordering in §3.

### 4.10 Every view but the first was starved — *critical*

Cleanliness was a property of the *source*. With several views over one source,
the first to refresh consumed the flag and the rest were skipped. An ordinary
"refresh all reports" loop after a single write:

```
truth=150   r1=150   r2=100   r3=100
```

`r2` and `r3` are silently frozen. This is fatal on an Odoo database, where many
report views sit on `account_move_line`. It cannot be patched at the edges — the
watermark has to be per view, which is what §3 now does.

### What the reject list covers

Refused rather than skipped: sequences (`nextval` touches no heap), system
catalogs (catalog writes are deliberately not tracked), `TABLESAMPLE`
(re-samples every refresh), virtual generated columns (`ALTER COLUMN ... SET
EXPRESSION` recomputes every value in place, with no rewrite and no
`relfilenumber` change), row-level security (policy changes), and any
non-`IMMUTABLE` function.

The `relkind` test is a **whitelist** — ordinary table, materialized view, or a
partitioned parent to expand — and is applied *before* the access-method test,
because a partitioned parent has `relam = 0` exactly like a view, a sequence and
a foreign table. Checking `relam` first would either reject every partitioned
source or admit sequences.

The `RTEKind` switch has **no `default:` arm**, so a range-table kind added in a
future release is a compile error rather than a silent hole.

---

## 5. Cost on the write path

The tracking hook sits in `heap_insert`, `heap_multi_insert`, `heap_delete` and
`heap_update` — the global write path, paid by every table whether or not any
view uses the feature.

| 2M-row bulk insert | |
|---|---|
| no materialized view anywhere | 2877 ms |
| with a tracked view on that table | 2901 ms |

0.9%, within run-to-run noise. When no view has ever tracked anything the hook
returns after two loads and two branches; otherwise it checks the most recent
relation first, which is the case that consecutive writes actually hit.

---

## 6. Usage

### Enabling

```sql
ALTER MATERIALIZED VIEW daily_sales SET (auto_skip_unchanged = true);
REFRESH MATERIALIZED VIEW daily_sales;   -- first one establishes the watermark
```

`CREATE MATERIALIZED VIEW ... WITH DATA` also establishes it, so the first
`REFRESH` after a create can already skip.

### Turning it off cluster-wide

```sql
SET dbblue_matview_skip_unchanged = off;   -- PGC_SUSET
```

Restores stock behaviour without having to locate every view that opts in.
Useful for comparing results or isolating a problem.

### Confirming a skip happened, or finding out why not

```sql
SET client_min_messages = debug1;
REFRESH MATERIALIZED VIEW daily_sales;
DEBUG:  matview "daily_sales": skipped, nothing it depends on has changed
```

When it declines, the line names the rule that fired — `a source was written`,
`the matview's definition changed`, `query calls a function that is not
IMMUTABLE`, and so on. That is the way to answer "why does this view never
skip", which is otherwise invisible: a view excluded by the reject list behaves
exactly like one whose sources are simply busy.

### The locking caveat

A skipped `REFRESH` still takes `AccessExclusiveLock` on the view and holds it
to end of transaction, because `ExecRefreshMatView` acquires it before
`RefreshMatViewByOid` is entered. **This removes the cost of the rebuild, not
the blocking.** Readers of the view are still locked out for the duration of the
surrounding transaction. Lowering that means moving the decision above name
resolution, which is a much larger change and was not attempted.

---

## 7. Testing

Everything lives in the tree and runs from the standard suites, so an upstream
merge that breaks the feature fails a build rather than going unnoticed.

| Where | What |
|---|---|
| `src/test/regress/sql/dbblue_matview_skip.sql` | the deterministic cases; one line in `parallel_schedule`, run by itself. **246/246** |
| `src/test/isolation/specs/dbblue-matview-skip.spec` | the concurrency cases. **131/131** |
| `src/test/recovery/t/056_dbblue_matview_skip.pl` | restart and crash behaviour. **18/18** |

Every case asserts **which path the refresh took**, via the DEBUG1 decision
line, not merely that the answer was right. That distinction matters: a test
that only checks contents still passes when the optimization never engages, so
it would not notice the feature silently doing nothing. The regression file
exercises all but a handful of the rejection reasons in the code; the remainder
are either concurrency-only (covered by the isolation spec) or unreachable by
construction and kept as defence in depth.

Three test-design traps hit while writing these, all worth remembering:

1. **`debug1` is not quiet.** It looked clean at first, but `REFRESH
   CONCURRENTLY` builds a transient toast index and logs its OID-derived name,
   which changes every run. That section is deliberately not run at `debug1`.
2. **A skipped refresh proves nothing about a race.** The first attempt at the
   commit-during-refresh case had the refresh under test skip, so it never took
   a snapshot and the window never opened. The source has to be dirtied first so
   the refresh genuinely rebuilds.
3. **Confirm a test can actually fail.** Of the original 38 standalone cases, 16
   were observed failing against the pre-fix code; the rest were written
   afterwards and had never been seen to fail. Asserting the decision line is
   what closed that gap.

Checking the partitioned case this way also found a latent problem: the
watermark is compared element by element, so the source set has to be built in
the same order every time. Inheritance expansion makes no ordering promise, so
the set is now sorted. Without that, a reordering would have made partitioned
matviews quietly stop skipping — passing every contents-only test.

## 8. Honest limitations

- **No real workload has run against this.** Everything above is synthetic. The
  failure mode — a report quietly not moving — is one an unwatched system will
  not report, which is exactly the combination that argues for a cautious
  rollout.
- **No Odoo validation.** The coverage question that matters for this fork is
  what fraction of real Odoo report views survive the §2 rejects. `now()`
  alone may exclude most of them. That number is not known.
- **Observability is DEBUG1 only.** A refresh logs its decision, and the reason
  when it declines, which is enough to diagnose "why does this never skip" and
  is what the tests assert on. There is still no aggregate view or counter, so
  there is no way to ask how often skipping is paying off across a workload.
- **`now()` has no opt-in escape.** The IVM work solved the same tension with an
  `allow_stable_keys` reloption. The equivalent here would let a view accept
  time-drift explicitly, and has not been added.
- **Watermarks live in shared memory and are lost on restart.** Fail-safe — the
  first refresh after a restart rebuilds — but it means a restart costs one full
  rebuild per view.
- **Fixed bounds**, all fail-safe: 1024 tracked sources per cluster, 256
  watermarks, 32 sources per view, 256 relations written per transaction.
  Exceeding any of them makes the affected view refuse to skip. Nothing evicts
  entries — not even when a matview or a source is dropped — so a cluster with
  more than 1024 distinct source tables, or one that churns through more than
  256 materialized views, will gradually stop skipping rather than start being
  wrong.
- **The SGML docs were not validated by `xmllint`**, which is not installed
  here. Well-formedness and every `linkend` target were checked directly, but
  the DTD validation `make -C doc/src/sgml check` performs has not run.
- **The TAP test needs `--enable-tap-tests`**, which in turn needs the
  `IPC::Run` Perl module. Neither was present here initially; both are required
  for `src/test/recovery` to run at all rather than report success without
  executing anything.

---

## 9. Production readiness

### Verified

- All eleven defects reproduced and fixed, each with a regression test.
- 38/38 feature, 245/245 core, 130/130 isolation.
- Concurrency: a row committed mid-refresh is retained; rollback, error-abort
  and savepoint-rollback all leave the view correctly marked as changed.
- Write-path cost is noise.

### Not verified

| Gap | Why it matters |
|---|---|
| **Real traffic** | Nothing has carried real users' load. |
| **Odoo coverage** | Unknown how many real report views clear the reject list. |
| **Crash / restart under sustained load** | Single-client restart and crash are covered by the TAP test, including a crash with a refresh still open. Nothing has been crashed while busy. |
| **Physical replication** | A standby cannot refresh, and a promoted standby starts with empty shared memory, so it should be fail-safe. Not tested. |

### Suggested rollout

1. Enable on **one** reporting view whose sources are demonstrably quiet, and
   confirm from timing that skips actually occur.
2. Check the view against a manually-refreshed copy for a few cycles — that is
   the only assertion that matters, and it is cheap.
3. Widen one view at a time. Since the reject list is silent, a view that never
   skips is indistinguishable from one that has nothing to skip; §8's
   observability gap is felt here first.
4. Keep `dbblue_matview_skip_unchanged` in mind as the single switch that
   reverts everything without touching any view definition.
