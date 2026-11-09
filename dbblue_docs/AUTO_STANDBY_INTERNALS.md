# DBblue — Auto Standby: Internals for Developers

This page explains **how the auto standby works inside the server**: which code runs,
in what order, what state it keeps, and why it is built the way it is. Read it before
you change [dbblue_create_standby.c](../src/backend/postmaster/dbblue_create_standby.c).

- For what the feature does for an operator (settings, Odoo setup, monitoring), read
  [AUTO_STANDBY.md](AUTO_STANDBY.md) first. This page assumes you know it.
- Function names below are the real ones. Search for them in the source; line numbers
  are left out on purpose, because they change with every commit.

---

## Contents

1. [The idea in one minute](#1-the-idea-in-one-minute)
2. [Where the code lives](#2-where-the-code-lives)
3. [The worker process](#3-the-worker-process)
4. [The main loop](#4-the-main-loop)
5. [One tick: `run_standby_tick()`](#5-one-tick-run_standby_tick)
6. [Building the standby](#6-building-the-standby)
7. [Starting the standby](#7-starting-the-standby)
8. [Watching a running standby](#8-watching-a-running-standby)
9. [Fencing in detail](#9-fencing-in-detail)
10. [Is this standby ours? Identity checks](#10-is-this-standby-ours-identity-checks)
11. [The replication slot](#11-the-replication-slot)
12. [Modes: on, paused, off](#12-modes-on-paused-off)
13. [Changing the standby directory](#13-changing-the-standby-directory)
14. [Shutdown and signals](#14-shutdown-and-signals)
15. [Running external commands: `run_command()`](#15-running-external-commands-run_command)
16. [Worker state (static variables)](#16-worker-state-static-variables)
17. [Files on disk](#17-files-on-disk)
18. [Settings and check hooks](#18-settings-and-check-hooks)
19. [Constants](#19-constants)
20. [Rules every change must keep](#20-rules-every-change-must-keep)
21. [Testing](#21-testing)
22. [How to make common changes](#22-how-to-make-common-changes)
23. [From a log line to the code](#23-from-a-log-line-to-the-code)

---

## 1. The idea in one minute

- One **background worker** runs inside the primary. It has **no database connection**.
- Every **10 seconds** it wakes up and makes sure a standby exists, runs, and keeps up.
- It does the real work by running the normal client programs through `/bin/sh`:
  - `pg_basebackup` to **build** the standby,
  - `pg_ctl` to **start** and **stop** it,
  - `pg_receivewal --drop-slot` to **drop** the replication slot.
- It reads shared memory directly (replication slots, walsenders) to see how the standby
  is doing.
- It writes small files so that others can find the standby:
  `<DataDir>/dbblue_standby.port` and `<DataDir>/dbblue_standby.dir`.
- It never trusts a directory it did not build, and **never deletes** data it cannot
  prove is its own.

```
            ┌─────────────────────────── primary (postmaster) ───────────────────────────┐
            │                                                                             │
 SIGHUP ───▶│  "dbblue create standby" bgworker      shared memory                        │
 SIGTERM ──▶│   DbblueCreateStandbyMain()  ◀──────── ReplicationSlotCtl (slot "dbblue_standby")
            │     │  every 10 s                       WalSndCtl (apply LSN of its walsender)
            │     ▼                                                                       │
            │   run_standby_tick() ──fork+sh──▶ pg_basebackup / pg_ctl / pg_receivewal    │
            │     │                                                                       │
            │     └─ writes <DataDir>/dbblue_standby.port, dbblue_standby.dir             │
            └─────────────────────────────────────────────────────────────────────────────┘
                                    │ pg_ctl start (own session)
                                    ▼
            ┌──────────── standby (separate postmaster, port P+1..P+10) ─────────────┐
            │ data dir: <DataDir>_standby   config: the PRIMARY's postgresql.conf,    │
            │ pg_hba.conf, pg_ident.conf  +  its own postgresql.auto.conf overrides   │
            │ streams from the primary through slot "dbblue_standby"                  │
            └─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Where the code lives

| File | What is there |
|---|---|
| [src/backend/postmaster/dbblue_create_standby.c](../src/backend/postmaster/dbblue_create_standby.c) | **Everything** of the worker: main loop, build, start, watch, fence, check hooks. About 5,600 lines, most of them comments that explain *why*. |
| [src/include/postmaster/dbblue_create_standby.h](../src/include/postmaster/dbblue_create_standby.h) | The mode enum `DbblueCreateStandbyMode`, the four GUC variables, `DbblueCreateStandbyRegister()` and `DbblueCreateStandbyMain()`. |
| [src/backend/postmaster/postmaster.c](../src/backend/postmaster/postmaster.c) | Calls `DbblueCreateStandbyRegister()` in `PostmasterMain()`, before preloaded libraries can take bgworker slots. |
| [src/backend/postmaster/bgworker.c](../src/backend/postmaster/bgworker.c) | Entry `DbblueCreateStandbyMain` in `InternalBGWorkers[]`, so the postmaster can find the function by name. |
| [src/backend/utils/misc/guc_parameters.dat](../src/backend/utils/misc/guc_parameters.dat) | Definitions of `dbblue_create_standby`, `dbblue_standby_directory`, `dbblue_standby_max_lag`, `dbblue_standby_user`. |
| [src/backend/utils/misc/guc_tables.c](../src/backend/utils/misc/guc_tables.c) | `dbblue_create_standby_options[]`: `on`/`off`/`paused`, plus hidden boolean spellings (`true`, `yes`, `1`…) because the setting used to be a boolean. |
| [src/include/utils/guc_hooks.h](../src/include/utils/guc_hooks.h) | Declares `check_dbblue_standby_directory()` and `check_dbblue_standby_user()`. |
| [src/backend/utils/activity/wait_event_names.txt](../src/backend/utils/activity/wait_event_names.txt) | Wait event `DBBLUE_CREATE_STANDBY_MAIN`. |
| [src/backend/backup/basebackup.c](../src/backend/backup/basebackup.c) | Excludes `dbblue_standby.port`, `dbblue_standby.dir`, `dbblue_standby.slot` and their `.tmp` files from every base backup. |
| [src/bin/pg_rewind/filemap.c](../src/bin/pg_rewind/filemap.c) | The same exclusions for `pg_rewind`. |
| [src/bin/initdb/auto_tune.c](../src/bin/initdb/auto_tune.c) | Sets `max_slot_wal_keep_size = 2 × max_wal_size`. The worker refuses to build without such a limit. |
| [src/test/modules/dbblue_standby/](../src/test/modules/dbblue_standby/) | TAP test `t/001_auto_standby.pl`. |
| [dbblue_docs/AUTO_STANDBY.md](AUTO_STANDBY.md) | Operator guide. |

Build wiring: `dbblue_create_standby.o` in
[src/backend/postmaster/Makefile](../src/backend/postmaster/Makefile) and
`dbblue_create_standby.c` in [src/backend/postmaster/meson.build](../src/backend/postmaster/meson.build).

---

## 3. The worker process

### Registration — `DbblueCreateStandbyRegister()`

Called once from `PostmasterMain()`. It registers a **static** background worker:

| Field | Value | Why |
|---|---|---|
| `bgw_flags` | `BGWORKER_SHMEM_ACCESS` only | No database connection. It must still work when `max_connections` is used up. |
| `bgw_start_time` | `BgWorkerStart_RecoveryFinished` | It is useless before the primary accepts writes. |
| `bgw_restart_time` | `60` | If it exits with code 1, the postmaster starts it again after 60 s. |
| `bgw_library_name` / `bgw_function_name` | `"postgres"` / `"DbblueCreateStandbyMain"` | Found through `InternalBGWorkers[]` in `bgworker.c`. |
| `bgw_name` / `bgw_type` | `"dbblue create standby"` | Shows in `ps`. |

It is **always** registered, even when the feature is off. The worker then just sleeps.

The one exception: **`IsBinaryUpgrade`** (during `pg_upgrade`) — then nothing is
registered. A standby built during `pg_upgrade` would be a copy of a half-restored
cluster on a temporary port.

### The worker never acts on a standby server

The standby runs on the primary's `postgresql.conf`, so it may see
`dbblue_create_standby = on` too. Two guards stop it from building a standby of its own:

1. The main loop does nothing while `RecoveryInProgress()` is true.
2. `write_standby_settings()` writes `dbblue_create_standby = off` into the standby's
   `postgresql.auto.conf`, so that even after a **promotion** it builds nothing until an
   operator says so.

### Error handling convention

The worker has **no `sigsetjmp` error handler**. An `ERROR` would kill the worker, and
it would only come back 60 s later. So in this file:

- Nothing raises `ERROR`. Every failure is reported as a **`WARNING`** and the function
  returns `false` (or a "failed" enum).
- Helpers that would `ERROR` in core are replaced by local versions. Example:
  `read_control_file()` instead of `get_controlfile()`; `copy_tde_file()` instead of
  `copy_file()`.
- A tick that returns `false` counts as **one failure** for the retry backoff
  (see [§4](#4-the-main-loop)).

### Memory convention

There is **no per-tick memory context**. Everything is allocated in the worker's
long-lived context, so every `palloc`/`psprintf` result is `pfree`d by hand. When you add
code, free what you allocate on **every** return path, or the worker slowly leaks.

---

## 4. The main loop

`DbblueCreateStandbyMain()` in short:

```c
install signal handlers: SIGTERM -> shutdown, SIGHUP -> reload,
                         SIGQUIT -> standby_command_quickdie()
before_shmem_exit(standby_command_atexit)

while (!ShutdownRequestPending)
{
    if (ConfigReloadPending)
        apply_config_reload();                 // see below

    if (!RecoveryInProgress())
    {
        if (mode != off)
        {
            warn_unlimited_slot_wal();         // max_slot_wal_keep_size = -1
            warn_synchronous_standby();        // synchronous_standby_names = '*'
        }

        if ((mode == on || mode != standby_mode_applied) && now >= retry_at)
        {
            if (mode == on)  ok = run_standby_tick();
            else             ok = set_standby_aside(drop_slot = (mode == off));

            ok ? reset failures : schedule retry with failure_retry_delay()
        }

        if (mode != off)
            release_idle_slot_xmin();          // every tick, even while backing off
    }

    WaitLatch(10 s, or until a signal sets the latch);
}

stop_standby_at_shutdown();
proc_exit(1);                                  // 1, not 0: see §14
```

### What runs when

| Mode | Every tick | Only when the mode changed |
|---|---|---|
| `on` | `run_standby_tick()`, `release_idle_slot_xmin()`, two warnings | — |
| `paused` | `release_idle_slot_xmin()`, two warnings | `set_standby_aside(false)` |
| `off` | nothing | `set_standby_aside(true)` |

`standby_mode_applied` remembers the mode last acted on, so `paused` and `off` act
**once**. If `set_standby_aside()` fails, `standby_mode_applied` is not updated, so it
is retried (with backoff).

### Retry and backoff — `failure_retry_delay()`

`consecutive_failures` counts ticks in a row that returned `false`.

| Failures in a row | Next attempt after |
|---|---|
| 1 – 4 | 10 s (the next tick) |
| 5 | 1 min |
| 6 | 2 min |
| 7 | 4 min |
| … | doubles … |
| 11 and later | 60 min (the cap) |

- The delay is counted from when the failed attempt **started** (`retry_at`).
- From the 5th failure on, the worker logs
  `"%d consecutive failures; retrying in %ld min"`.
- It **never gives up**. A full disk at night clears by itself once space is free.

### Reload — `apply_config_reload()`

Runs on `SIGHUP`, from the main loop **and** from inside `run_command()` during a
build. It:

1. calls `ProcessConfigFile(PGC_SIGHUP)`;
2. resets `consecutive_failures` and `retry_at` → **a reload retries at once**;
3. clears every "warned once" flag (`slot_wal_limit_warned`, `sync_standby_warned`,
   `space_short_warned`, `tablespace_warned`) and `space_recheck_at`;
4. sets `standby_reload_pending`, so the next tick also reloads the standby
   (`reload_standby()`).

### Return values that are not failures

`run_standby_tick()` returns `true` (no backoff) in some cases where no standby is
running, because waiting is the right thing to do and the reason was already logged:

- the cluster has **tablespaces** (`standby_copyable()`);
- **not enough disk space** (`standby_has_room()`, which itself rechecks only once a
  minute);
- an interrupted build whose slot is still **in use** (a `pg_basebackup` still copying);
- a build **cancelled** by a reload (`COMMAND_CANCELLED`).

---

## 5. One tick: `run_standby_tick()`

This is the heart of the feature. One call goes through these steps, top to bottom.
Any step can `return false` (failure) or `return true` (done for this tick).

```
run_standby_tick()
│
├─ 1. dir = resolve_standby_directory()          too long / shell chars / inside DataDir → false
├─ 2. logfile = standby_log_path(dir)            shell chars → false
│     adopt_legacy_slot_owner()                  older version: record the slot's directory (§10)
├─ 3. retire_previous_standby(dir)               stop the standby in the OLD directory (§13)
├─ 4. build marker "<dir>.building" owned by another DataDir?  → warn, false
├─ 5. standby_build_interrupted(dir)?            our own crashed build:
│        slot ACTIVE → wait (true)
│        else rmtree(dir contents), unmark → continue
├─ 6. has PG_VERSION && !vet_existing_standby()  not ours / promoted / unusable → false
│        (vet may also wipe dir for a rebuild → continue: slot invalidated,
│         absent, or made for another directory)
├─ 7. fenced = standby_fenced(dir)               hot_standby = off in its auto.conf?
│
├─ 8. RUNNING?  standby_is_running(dir)
│     ├─ refence = standby_fence_changes(dir, fenced)
│     ├─ no refence and !standby_limits_behind(dir):
│     │      remember fence, reload_standby() if pending, sync_standby_port() → true   ◀── normal case
│     └─ else: flip fence if needed, forget port, stop_standby() → fall through to START
│
├─ 9. standby_parent_usable(dir)                 parent missing / not writable → false
│
├─ 10. BUILD if no PG_VERSION   (§6)
│      ├─ dir exists but not empty → false;  fix its permissions to 0700
│      ├─ max_slot_wal_keep_size = -1 → false
│      ├─ standby_copyable()  (no tablespaces)   else true (wait)
│      ├─ standby_has_room()                     else true (wait)
│      ├─ drop_standby_slot()                    left-over slot
│      ├─ record_standby_slot_owner(dir)         write dbblue_standby.slot
│      ├─ mark_standby_build()                   write "<dir>.building"
│      ├─ seed_standby_tde_dir() if WAL encrypted
│      ├─ run_command("pg_basebackup … -C -S dbblue_standby -R -w [-E]", build_dir=dir)
│      │     failed → clean dir (unless shutting down), drop slot → false (true if cancelled)
│      └─ unmark_standby_build()
│
└─ 11. START   (§7)
       ├─ port = find_free_port(standby_previous_port(dir))      none → false
       ├─ warn if the port moved
       ├─ fenced = pending fence, or slot backlog > dbblue_standby_max_lag
       ├─ write_standby_settings(dir, port, fenced)
       ├─ reset fence/lag tracking, forget_standby_port()
       ├─ remove a stale postmaster.pid (only if surely stale)
       ├─ run_shell("pg_ctl -D dir -l log -o '<opts>' -w -t 60 start")
       ├─ verify it is really running
       └─ fenced ? log "refusing connections" : report_standby_port()
```

The **normal steady state** is step 8: the standby is running, nothing changes, and the
tick ends after a few cheap checks.

---

## 6. Building the standby

Reached when the standby directory holds no `PG_VERSION`.

### Preconditions, in order

| Check | Function | On failure |
|---|---|---|
| Parent of `dir` exists and is writable | `standby_parent_usable()` | `false`. A missing parent is **never created**: a typo must not grow a full copy of the database somewhere. |
| `dir`, if it exists, is empty (a lone `pg_tde/` counts as empty — our own seed) | `standby_dir_is_empty()` | `false`, "exists but is not a PostgreSQL cluster". |
| `dir` has no group-write/other access | `chmod(dir, 0700)` | `false`. `pg_basebackup` keeps the mode, and postgres refuses to start in a group/world-accessible data dir. |
| `max_slot_wal_keep_size` is not `-1` | — | `false`. Without a limit the slot could fill the disk and stop the primary. |
| No tablespaces in `pg_tblspc` | `standby_copyable()` | `true` (wait), warns once. On the same host, `pg_basebackup` would write each tablespace to the primary's own directory. |
| Enough free disk | `standby_has_room()` | `true` (wait), warns once, rechecks every 60 s. |

### Disk space rule — `standby_has_room()`

```
needed = size of DataDir (without pg_wal, pg_replslot, temp files; symlinks not followed)
       + max_wal_size                          (WAL streamed during and after the copy)
       + reserve, only if the standby shares a filesystem with DataDir or pg_wal:
             max(10% of that filesystem, 2 × max_wal_size)
```

If `statvfs()` fails, the build goes ahead (old behaviour).

### The build itself

1. **`drop_standby_slot()`** — any slot named `dbblue_standby` left now belongs to a
   standby that no longer exists. Left there it would hold WAL, and
   `pg_basebackup -C` would fail with "already exists". An **active** slot is not
   dropped (someone else streams from it): warn and fail.
2. **`record_standby_slot_owner(dir)`**: writes `dir` to `<DataDir>/dbblue_standby.slot`.
   From now on any slot named `dbblue_standby` is `dir`'s, so a standby left in another
   directory is rebuilt rather than started if the setting points back at it (§10).
3. **`mark_standby_build()`** — writes `<dir>.building` containing the primary's
   `DataDir`, and fsyncs it and its parent. From now on, until the marker is removed,
   whatever is in `dir` is "ours, and maybe half-built".
4. **`seed_standby_tde_dir()`** — only if `<DataDir>/pg_tde/wal_keys` exists (WAL
   encryption is on). Copies `pg_tde/wal_keys` and `pg_tde/1664_providers` into
   `<dir>/pg_tde`, so that `pg_basebackup -E` can find the server key before the copy
   starts.
5. **`pg_basebackup`** through `run_command()` with `build_dir = dir`:

   ```
   pg_basebackup -h <first socket dir | localhost> -p <PostPortNumber> [-U <dbblue_standby_user>]
                 -D <dir> -X stream -c fast [-E] -C -S dbblue_standby -R -w
   ```

   | Flag | Why |
   |---|---|
   | `-X stream` | Streams the WAL written during the copy. |
   | `-c fast` | Immediate checkpoint, no waiting for the next one. |
   | `-E` | pg_tde: re-encrypts the streamed WAL, so the standby can read it. |
   | `-C -S dbblue_standby` | Creates the slot **before** copying, so no WAL the standby needs is ever recycled. |
   | `-R` | Writes `standby.signal`, `primary_conninfo` and `primary_slot_name`. |
   | `-w` | Never prompt for a password (there is no terminal). |

   The host is the **first Unix socket directory**, because the default
   `local replication … peer` rule in `pg_hba.conf` lets the OS user in without a
   password. `-R` copies the same host into `primary_conninfo`, so the standby streams
   over the socket too.
6. On **success**: `unmark_standby_build()`, then continue to start (§7).
7. On **failure**:
   - while shutting down: leave the partial copy and the marker; the next start clears
     it (step 5 of the tick). Deleting gigabytes now would hold up the shutdown.
   - otherwise: `rmtree(dir, false)` (contents only — the directory may be the
     operator's), remove the marker;
   - `drop_standby_slot()` — `-C` already made the slot;
   - return `false`, or `true` if it was `COMMAND_CANCELLED` (a reload no longer wants
     this build — not a failure).

### Interrupted builds — the `.building` marker

A crash or `SIGKILL` (of the primary, the worker, or `pg_basebackup`) runs no cleanup.
The next tick would find a half copy, possibly with `PG_VERSION`, and refuse it forever
as "not ours". The marker fixes that:

| Marker found | Meaning | What the tick does |
|---|---|---|
| none | no build in progress | nothing |
| contains our `DataDir`, or empty (older version) | our build was interrupted | if the slot is active → wait; else wipe `dir`, remove marker, rebuild |
| contains another `DataDir` | a **copy of this server** builds into the same absolute directory | warn, leave everything alone, `false` |

---

## 7. Starting the standby

Reached after a build, after a stop for a restart, or when the standby is not running.

### 7.1 Choosing the port — `find_free_port()`

The standby port must stay the same across restarts, because Odoo's `db_replica_port` is
set by hand.

1. **Preferred port** = `port` in the standby's own `postgresql.auto.conf`
   (`standby_previous_port()`), i.e. the port it was last started on. A fresh copy holds
   the **primary's** port there, which is outside the range and so ignored.
2. **Range**: `PostPortNumber + 1` … `PostPortNumber + 10`.
3. **Claimed ports** (`collect_configured_ports()`): ports that **other clusters are
   configured for**, even while they are stopped:
   - every `/etc/postgresql/<version>/<cluster>/postgresql.conf` (Debian);
   - every data directory **next to ours** (its `postgresql.auto.conf`, else
     `postgresql.conf`, else the default 5432) — except a standby of ours
     (`sibling_is_our_standby()`).
4. **Free test** (`port_is_free()`), all three must pass:
   - `bind()` on `127.0.0.1` with `SO_REUSEADDR` (so a port in `TIME_WAIT` after a
     standby restart still counts as free);
   - `bind()` at every address in `listen_addresses` (`listen_port_taken()`);
   - no live process holds `<socket dir>/.s.PGSQL.<port>.lock`
     (`socket_port_in_use()`).
5. The preferred port wins if it is in range, unclaimed and free; else the first free one.
   If the port moved, log `"standby moves from port X to port Y"` with a hint for Odoo.

There is still a race: something may take the port between the probe and the bind. The
start then fails, and the next tick picks another port.

### 7.2 Deciding whether to start fenced

```c
if (standby_fence_pending >= 0)          // a fence change whose restart failed earlier
    fenced = standby_fence_pending;
if (!fenced && dbblue_standby_max_lag > 0 &&
    standby_slot_backlog() > dbblue_standby_max_lag MB)   // flush LSN - slot restart_lsn
    fenced = true;                       // log "starting it to refuse connections"
```

So a standby coming back after a pause, a crash or a reboot with gigabytes to replay
starts **closed**, and is opened once it has caught up (§9).

### 7.3 Writing its settings — `write_standby_settings(dir, port, fenced)`

The standby is started with the **primary's** config files (§7.5). Its own
`postgresql.auto.conf` overrides what must differ. This function rewrites that file
**before every start** and **before every reload**:

1. Read the current file.
2. Drop every line that sets one of the keys below (so they never pile up).
3. Keep only the **last** `primary_conninfo` and `primary_slot_name` (the ones `-R`
   just added; older ones come from a chain of promotions).
4. Keep every other line (pg_tde settings, `default_table_access_method`, …).
5. Append:

| Setting | Value | Why |
|---|---|---|
| `port` | the chosen port | — |
| `unix_socket_directories` | the primary's **live** value | It may come from the command line (`pg_ctl -o "-k …"`), which `pg_basebackup` cannot copy. |
| `hot_standby_feedback` | `on` | Long Odoo reports on the standby are not cancelled by VACUUM on the primary. |
| `hot_standby` | `off` if fenced, else `on` | **This is the fence.** Written explicitly `on` too, because an `off` in the shared `postgresql.conf` is harmless on a primary but would lock the standby. |
| `cluster_name` | `'dbblue_standby'` | Its `application_name` in `pg_stat_replication`, and the name `synchronous_standby_names` would have to match. |
| `dbblue_create_standby` | `off` | A promoted standby must not build standbys of its own. |
| `dbblue_standby_directory` | `''` | The copied value names the standby's own directory; a parallel worker's check hook would then fail every parallel query. |
| `log_filename` | ours, or `standby-<ours>` when `log_directory` is absolute | Two servers must not append to the same log file. |
| `shared_buffers`, `work_mem`, `effective_cache_size` | half of ours, not below the PostgreSQL default unless ours is lower (`standby_memory_kb()`) | Both servers share one machine sized for one. |
| `max_connections`, `max_worker_processes`, `max_wal_senders`, `max_prepared_transactions`, `max_locks_per_transaction` | `max(our live value, value in the standby's pg_control)` (`standby_limit_values()`) | A hot standby must have these **at least** as high as the primary, or replay pauses and it refuses to restart. |

6. Write `postgresql.auto.conf.tmp`, then `durable_rename()` it into place.
7. Log any limit that was raised.

Values are quoted with `append_conf_string()`, which doubles both `'` and `\`, as
`ALTER SYSTEM` does.

### 7.4 Before `pg_ctl start`

- Reset fence/lag tracking: `standby_last_apply`, `standby_last_flush`,
  `standby_stalled_since`, `standby_lagging_ticks`.
- Set `standby_running_fenced = fenced`, clear `standby_fence_pending` and
  `standby_reload_pending` (a start reads all config anyway).
- `forget_standby_port()` — nothing may be sent to the standby until it is up.
- **Stale `postmaster.pid`** — `standby_pid_state()`:

  | State | Meaning | Action |
  |---|---|---|
  | `NONE` | no file | start |
  | `GONE` | PID ≤ 1, or no such process | remove file, start |
  | `OTHER` | PID belongs to another user, or (Linux) its `/proc/<pid>/cwd` is not `dir` | remove file, start |
  | `UNKNOWN` | could not tell | **do not start**, warn, `false` |
  | `RUNNING` | the standby is alive | (not reached here) |

  Removing the file when unsure could start a **second postmaster** on the same data
  directory, so `UNKNOWN` is never treated as stale.

### 7.5 `pg_ctl start`

```
pg_ctl -D <dir> -l <logfile> -o '<server opts>' -w -t 60 start
```

`standby_server_opts()` builds the server options:

```
-c config_file=<primary's>  -c hba_file=<primary's>  -c ident_file=<primary's>
-c data_directory=<dir>     [-c external_pid_file=<dir>.pid]
```

On Debian the config files live in `/etc/postgresql/...`, not in the data directory, so
a plain copy would have none. `data_directory` and `external_pid_file` are overridden
because the primary's `postgresql.conf` may set them to the primary's paths.

### 7.6 After `pg_ctl start`

- `pg_ctl` reports success even if a standby that was cleanly shut down in recovery
  exits right away. So the worker checks `standby_is_running()` and, if not, warns
  `"exited during startup"` with an excerpt of the log.
- `pg_ctl` gives up after 60 s, but the standby may still be replaying. Then the worker
  logs `"is still starting"`. `sync_standby_port()` reports the port on a later tick.
- Success, not fenced → `report_standby_port()`: log line with an Odoo hint, and write
  `<DataDir>/dbblue_standby.port` (via `.tmp` + `durable_rename()`).
- Success, fenced → only a log line. **No port file while fenced.**

---

## 8. Watching a running standby

When `standby_is_running(dir)` is true, a tick does:

```
refence = standby_fence_changes(dir, fenced)        // §9
if (!refence && !standby_limits_behind(dir))
{
    standby_running_fenced = fenced;
    if (standby_reload_pending && reload_standby(dir, fenced))
        standby_reload_pending = false;
    if (!fenced)
        sync_standby_port(dir);
    return true;                                     // nothing else to do
}
// else: restart it (stop now, start in the same tick)
```

| Check | Function | What it does |
|---|---|---|
| Fence must change? | `standby_fence_changes()` | §9. |
| Limits too low? | `standby_limits_behind()` | Compares `max_connections` and the 4 others in the standby's `postgresql.auto.conf` with what it needs. Too low, or not set at all (built by an older version) → restart. The start writes the right values. |
| Reload | `reload_standby()` | After a primary reload: rewrites the standby's settings, then sends `SIGHUP` to the PID in its `postmaster.pid`. Only done when **no restart** is needed, because rewriting first would hide from `standby_limits_behind()` that it runs with too little. |
| Port file | `sync_standby_port()` | If the standby's `postmaster.pid` says it is ready (`PM_STATUS_READY` or `PM_STATUS_STANDBY`) and the port file disagrees or is missing, rewrite it. Covers the "pg_ctl gave up after 60 s" case. |

**Why reload matters:** only the primary's postmaster gets `pg_reload_conf()`. Without
`reload_standby()`, a `pg_hba.conf` rule that locks a role out would still let it read
everything from the standby until its next restart.

**What does not reach the standby:** `ALTER SYSTEM` on the primary. It lands in the
primary's `postgresql.auto.conf`; the standby reads its own.

---

## 9. Fencing in detail

**Goal:** Odoo must never read stale data. A standby that is far behind, or not
replaying, is restarted with `hot_standby = off`. It keeps streaming and replaying but
refuses every connection, so Odoo falls back to the primary.

### 9.1 Measuring progress — `standby_replication()`

Each tick:

1. `standby_apply_lsn()` finds the PID holding slot `dbblue_standby`, then the walsender
   with that PID in `WalSndCtl`, and reads its `apply` LSN. Matching by slot holder means
   other standbys or `pg_receivewal` are never mistaken for ours.
2. It saves `standby_last_apply = apply` and `standby_last_flush = GetFlushRecPtr()`, and
   compares with the **previous** tick's values:

| Result | Condition |
|---|---|
| `DISCONNECTED` | no walsender holds our slot |
| `BEHIND` | first look since start (no previous flush), or apply moved but is short of last tick's flush |
| `CAUGHT_UP` | apply ≥ the flush LSN of **one tick ago** |
| `STALLED` | streaming, but apply did not move and is short of last tick's flush |

"Caught up" compares with the previous tick's flush, not the current one. A busy
primary keeps writing, so the current flush is almost never reached.

### 9.2 Deciding — `standby_fence_changes(dir, fenced)`

```
LAG CHECK, first, for BEHIND, STALLED and DISCONNECTED:
    lag = flush - apply                    (BEHIND, STALLED: apply from the walsender)
    lag = flush - slot restart_lsn         (DISCONNECTED: what it received, the most
                                            it can have replayed)
    if (!fenced && max_lag > 0 && lag > max_lag MB):
        if (++lagging_ticks >= 2)                 // 2 checks in a row: no one-tick spike
            WARNING "... MB behind ..., restarting it to refuse connections"
                    (hint: "Its replay has stopped" when STALLED;
                     "does not stream from this server, and is at least … MB behind it"
                     when DISCONNECTED)
            return true                           // → fence
    else
        lagging_ticks = 0

CAUGHT_UP or BEHIND:
    stalled_since = 0
    if (fenced && CAUGHT_UP):
        LOG "has caught up ..., restarting it to accept connections again"
        return true                               // → unfence
    return false

STALLED or DISCONNECTED:
    stalled_since = stalled_since ?: now
    if (fenced || less than 60 s) return false
    WARNING "has not streamed ..." / "has not replayed past ..."
    return true                                   // → fence
```

The lag check runs for a **stalled** standby too. A stopped replay falls behind at the
full rate of the primary's writes. A lock conflict that `max_standby_streaming_delay`
(30 s by default) cuts short never reaches the 60 s timer, so before this, such a
standby served data far more than `max_lag` behind and was never fenced.

A **disconnected** standby has no walsender, so its replay position is unknown. But the
slot's `restart_lsn` (`standby_slot_restart_lsn()`) still holds what it last received,
and it cannot have replayed past that, so `flush − restart_lsn` is the **smallest** its
lag can be. When even that is over `max_lag`, it is fenced. A standby that reconnects
before the second check is measured normally again and is not fenced for a short break.

The 60 s timer still covers a standby that has stopped or disconnected but is **not**
far behind (a quiet database).

### 9.3 The fence state machine

```
        lag > max_lag on 2 ticks in a row (replaying or stalled)
                    or stalled / disconnected ≥ 60 s
   ┌──────────┐  ─────────────────────────────────────▶  ┌──────────┐
   │  OPEN    │                                          │  FENCED  │
   │ hot_sb=on│  ◀─────────────────────────────────────  │hot_sb=off│
   └──────────┘        CAUGHT_UP (needs streaming)       └──────────┘
        ▲                                                     ▲
        │ start: backlog ≤ max_lag                            │ start: backlog > max_lag
        └──────────────────── (start, §7.2) ──────────────────┘
```

- Each arrow is a **restart** (`stop_standby()` then the start path), so open replica
  connections are dropped.
- A disconnected fenced standby **stays fenced** until it can stream again.
- A restart often fixes a stall by itself: a replay paused with `pg_wal_replay_pause()`
  does not survive a restart.

### 9.4 Where the fence is stored

- **On disk:** `hot_standby` in the standby's `postgresql.auto.conf`.
  `standby_fenced(dir)` reads it back at every tick, so the fence survives restarts of
  the standby, the primary and the worker.
- **In memory:**
  - `standby_running_fenced` — whether the standby we last saw running is fenced. Used
    by `release_idle_slot_xmin()`.
  - `standby_fence_pending` — the fence a restart was **meant** to apply when
    `stop_standby()` failed (e.g. `pg_ctl` gave up after 60 s while the standby still
    stops). Without it, the next start would read the **old** fence back from disk.
    `-1` = none, `0`/`1` = open/fenced.

### 9.5 Side effects of a fence

- `forget_standby_port()` — the port file is removed, so tools stop pointing at it.
- `release_idle_slot_xmin()` — a fenced standby sends no `hot_standby_feedback`, so the
  slot's `xmin` is cleared at once and VACUUM on the primary is no longer held back.

---

## 10. Is this standby ours? Identity checks

Before the worker starts, reports, stops or deletes anything in a directory, it asks
`standby_identity(dir)`:

```
read pg_control (read_control_file, retries a bad CRC 10×)
  └─ unreadable                                 → UNREADABLE
system_identifier != ours                        → FOREIGN
no standby.signal                                → PROMOTED
primary_conninfo does not point at us            → OTHER_PRIMARY   (standby_follows_us)
primary_slot_name != 'dbblue_standby'            → NOT_BUILT
otherwise                                        → OURS
```

`standby_follows_us()` parses `primary_conninfo` with `conninfo_value()` (libpq quoting
rules; the last keyword wins), falls back to `PGHOST`/`PGPORT`/defaults, and then:

- the port must equal `PostPortNumber`;
- for a socket host, it must be one of our `unix_socket_directories`;
- for a TCP host, we must have a non-empty `listen_addresses`;
- a multi-host conninfo, or none at all, is given the benefit of the doubt.

**Why so strict?** A copy of this server (a restored backup, a hand-made
`pg_basebackup`, a copied data directory) has the **same system identifier and the same
settings**. On the same host it would see our standby as its own, report it to its Odoo,
and restart it on its own port. The port and socket tell the two apart.

### What `vet_existing_standby()` does with each answer

| Identity | Action | Returns |
|---|---|---|
| `OURS` | check the slot (below) | — |
| `PROMOTED` | forget port, warn, **drop our slot** (nothing will stream from it again). Leave it running: it is someone's primary now. | `false` |
| `FOREIGN` | forget port, warn with both system identifiers | `false` |
| `OTHER_PRIMARY` | forget port, warn. Hint: if **our** port or socket changed, edit `primary_conninfo` and reload instead of rebuilding. | `false` |
| `NOT_BUILT` | forget port, warn: a hand-made standby (maybe delayed on purpose) is never touched | `false` |
| `UNREADABLE` | (already warned) | `false` |

For `OURS`, the slot decides:

| Slot | Action |
|---|---|
| `IDLE` or `ACTIVE`, made for this directory (or owner unknown) | fine → `true` |
| `ACTIVE`, made for **another** directory | forget port, warn: the standby there still streams through it → `false` |
| `IDLE`, made for **another** directory | treated as `REPLACED` (below) |
| `INVALIDATED`, `ABSENT` or `REPLACED`, and `max_slot_wal_keep_size = -1` | forget port, **stop** the standby, keep its files (no rebuild is possible) → `false` |
| `INVALIDATED` | warn with the reason (`RS_INVAL_IDLE_TIMEOUT` → raise `idle_replication_slot_timeout`; `RS_INVAL_WAL_REMOVED` → fell behind `max_slot_wal_keep_size`), stop, `rmtree` contents → `true` (rebuild) |
| `ABSENT` | log, stop, `rmtree` contents → `true` (rebuild) |
| `REPLACED` | warn `was made for the standby in …, not for this one`, stop, `rmtree` contents → `true` (rebuild) |

During shutdown it stops before `rmtree`; the next start finds the same slot state and
finishes the job.

### Which standby the slot belongs to — `dbblue_standby.slot`

The slot is known only by its **name**. A build in another directory drops it and makes a
new one with the same name. So "a slot named `dbblue_standby` exists" does not mean
"this standby's slot exists". `<DataDir>/dbblue_standby.slot` records which directory the
current slot was made for:

- **Written** by `record_standby_slot_owner(dir)` in the build path, right after
  `drop_standby_slot()` and before `pg_basebackup -C`. From that moment, any slot of that
  name is `dir`'s, whether the build then succeeds or not.
- **Read** by `standby_slot_owner()` in `vet_existing_standby()`. If it names another
  directory, the standby here lost its slot when that other standby was built. The WAL
  it needs has not been kept since, so it is gone after a checkpoint or two. The slot
  state becomes `STANDBY_SLOT_REPLACED`, which only `vet_existing_standby()` uses
  (`standby_slot_state()` never returns it).
- **Missing** (a cluster upgraded from an older version): `adopt_legacy_slot_owner()`
  runs at the start of each tick, **before** `retire_previous_standby()` rewrites
  `dbblue_standby.dir`. If a slot exists, it records the directory `dbblue_standby.dir`
  names, which is where the older version built the slot. If that guess is wrong (the
  directory was changed but no build followed), a standby that could have resumed is
  rebuilt. That costs a copy but never serves stale data. With neither file, the owner
  stays unknown and the slot is taken for ours, as before.

**Why it matters (the bug it fixes):** before this file, pointing
`dbblue_standby_directory` back at a standby set aside for one built elsewhere
accepted the old standby, because a slot of that name existed. The start-time backlog
was measured from the slot, that is from the **other** standby's position, so the old
standby started **open** and served Odoo its old data. It could not stream, because its
WAL was gone, so after 60 s the stall fence closed it. It then stayed fenced for good,
never rebuilt, while the slot held WAL back.

---

## 11. The replication slot

Name: `dbblue_standby` (`DBBLUE_STANDBY_SLOT`). Physical. Created by
`pg_basebackup -C`; dropped by `pg_receivewal --drop-slot` in `drop_standby_slot()`.

### Reading it — always from shared memory

All reads take `ReplicationSlotControlLock` shared and the slot's spinlock:

| Function | Returns |
|---|---|
| `standby_slot_state()` | `ABSENT` / `IDLE` / `ACTIVE` (`active_proc` set) / `INVALIDATED` |
| `standby_slot_invalidation()` | `slot->data.invalidated` (the cause) |
| `standby_slot_backlog()` | our flush LSN − `restart_lsn` (WAL the standby still must receive) |
| `standby_apply_lsn()` | the apply LSN from the walsender holding the slot |

### Dropping it — `drop_standby_slot()`

- Skipped during shutdown (the next build drops it anyway).
- Waits up to 5 s for the walsender of a just-stopped standby to let go.
- Still `ACTIVE` → warn ("in use by another process") and fail. We never drop a slot
  someone streams from.

### Releasing its hold on VACUUM — `release_idle_slot_xmin()`

With `hot_standby_feedback = on`, the walsender stores the standby's oldest needed xmin in
the slot. If the standby stops sending feedback (stopped, paused, fenced, cannot connect),
the slot keeps the old xmin until it is invalidated — possibly weeks on a quiet database,
with no dead row removed.

So, every tick while the mode is `on` or `paused`:

| Slot state | Action |
|---|---|
| `ACTIVE` and `standby_running_fenced` | clear xmin **now** (a fenced standby runs no queries) |
| `IDLE` for ≥ 60 s (`slot_idle_since`) | clear xmin |
| otherwise | nothing |

`clear_standby_slot_xmin()` does it by hand, as no SQL function can:

- takes `ReplicationSlotControlLock` **exclusively** (instead of
  `ReplicationSlotAcquire()`, which would `ERROR` if the standby grabs the slot at that
  moment);
- sets `xmin`, `effective_xmin`, `catalog_xmin`, `effective_catalog_xmin` to invalid and
  marks the slot dirty;
- calls `ReplicationSlotsComputeRequiredXmin(false)`.

`restart_lsn` is **not** touched, so the WAL is still kept and the standby can resume.

---

## 12. Modes: on, paused, off

`set_standby_aside(drop_slot)` handles `paused` (`false`) and `off` (`true`), once per
change:

1. Find the directory from `<DataDir>/dbblue_standby.dir` (the one **we** last ran a
   standby in). With no such file, there is nothing of ours to stop — `off` is the
   default, and a standby someone else put in `<DataDir>_standby` is not ours.
2. `forget_standby_port()`; reset `standby_running_fenced` and `standby_fence_pending`.
3. If that directory holds a running standby whose identity is `OURS` → `stop_standby()`.
4. If `off` → `drop_standby_slot()`.
5. Files are **always** left in place.

| | `paused` | `off` |
|---|---|---|
| Standby process | stopped | stopped |
| Slot | kept (WAL kept up to `max_slot_wal_keep_size`; xmin released after 60 s) | dropped |
| Files | kept | kept |
| Back to `on` | resumes streaming; starts fenced if the backlog > `max_lag`; rebuilt if the slot was invalidated meanwhile | `vet_existing_standby()` sees `ABSENT` slot → wipes and rebuilds |

A standby that the operator starts by hand **after** `paused`/`off` is left alone,
because those modes act only once.

---

## 13. Changing the standby directory

`<DataDir>/dbblue_standby.dir` holds the absolute path of the directory last managed.
`retire_previous_standby(dir)` runs at the start of every tick:

- Same as `dir` → nothing.
- Different:
  1. `forget_standby_port()` (the port file named the old standby);
  2. if the old directory holds a **running** standby whose identity is `OURS` → stop it,
     and log a hint to remove its files by hand;
  3. write the new path to `dbblue_standby.dir`.

**Why:** otherwise the old standby keeps streaming from the one slot. Then
`drop_standby_slot()` refuses to drop it, and the new standby is never built. Or both
take turns on the slot and one always serves stale data.

**Pointing back at the old directory** rebuilds the standby there, if a standby was built
in the new directory meanwhile: that build dropped the old standby's slot. See
[Which standby the slot belongs to](#which-standby-the-slot-belongs-to--dbblue_standbyslot).
If no build happened in between (for example, it waited for disk space), the slot is
still the old standby's, and it simply resumes.

During a build, `run_command()` notices the change on reload
(`standby_build_wanted()`) and cancels `pg_basebackup` at once.

---

## 14. Shutdown and signals

### Signals the worker handles

| Signal | Handler | Effect |
|---|---|---|
| `SIGHUP` | `SignalHandlerForConfigReload` | `ConfigReloadPending` → `apply_config_reload()` |
| `SIGTERM` | `SignalHandlerForShutdownRequest` | `ShutdownRequestPending` → leave the loop |
| `SIGQUIT` | `standby_command_quickdie()` | forward `SIGQUIT` to the running command's process group, then `_exit(2)` |
| postmaster death | `WL_EXIT_ON_PM_DEATH` / `WL_POSTMASTER_DEATH` | `proc_exit(1)`; `standby_command_atexit()` sends `SIGTERM` to the command's group |

### `stop_standby_at_shutdown()`

Called after the loop ends on `SIGTERM`.

- Only if `standby_mode_applied == ON`, and the directory in `dbblue_standby.dir` holds a
  running standby whose identity is `OURS`.
- `pg_ctl` cannot be used here (`run_command()` refuses to start anything once shutdown
  is requested). So it reads the PID from the standby's `postmaster.pid`, sends
  **`SIGINT`** (fast shutdown), and polls up to 60 s for the file to disappear.
- Removes the port file first.

**Why stop it at all?** Under systemd the standby lives in the primary's cgroup.
`dbblue@.service` (`KillMode=mixed`) would `SIGKILL` it → crash recovery next time.
Debian's `postgresql@.service` would send `SIGTERM` (smart shutdown) → waits for every
Odoo read session. Doing it ourselves makes it clean and the same everywhere.

An **immediate** shutdown or a crash of the primary does not come here: the standby keeps
running, and the next start of the worker picks it up.

### Exit code 1, on purpose

The worker always ends with `proc_exit(1)`. The postmaster **unregisters** a worker that
exits with 0. If an operator sends `SIGTERM` to the worker alone (`kill <pid>`), exit 0
would leave the standby stopped until the next server restart. With 1, the postmaster
restarts it after `bgw_restart_time` (60 s), and it starts the standby again. During a
real server shutdown no worker is restarted, whatever the code.

The worker cannot tell those two `SIGTERM`s apart, so in both cases it stops the
standby.

---

## 15. Running external commands: `run_command()`

`run_command(cmd, logfile, what, build_dir)` returns `COMMAND_OK`, `COMMAND_FAILED`,
`COMMAND_INTERRUPTED` or `COMMAND_CANCELLED`. `run_shell()` is the same with
`build_dir = NULL`, returning a `bool`.

**Why not `system()`?** A base backup of a large Odoo database takes hours. `system()`
would block the worker: a reload turning the feature off would only be seen after the
copy finished (and the standby then started anyway), and `SIGQUIT` would be ignored.

### How it works

```
if ShutdownRequestPending → COMMAND_INTERRUPTED (start nothing new)
full = "<cmd> >> '<logfile>' 2>&1"
fork()
  child:  setpgid(0,0); reset signals to SIG_DFL; unblock all; exec /bin/sh -c full
  parent: setpgid(pid,pid); command_pgid = pid
loop:
  waitpid(WNOHANG) → exited? break
  if not yet asked to stop:
      shutdown pending              → stopped = INTERRUPTED
      build_dir && reload pending   → apply_config_reload();
                                      !standby_build_wanted(build_dir) → stopped = CANCELLED
      if stopped: kill(-pgid, SIGTERM); kill_at = now + 5 s
  else if now ≥ kill_at: kill(-pgid, SIGKILL)
  WaitLatch(100 ms)   (postmaster death → proc_exit(1))
after: if stopped, kill(-pgid, SIGKILL) to catch anything left; command_pgid = 0
       re-set our latch if a shutdown/reload is still pending
```

- **Own process group:** so the whole tree can be signalled. `pg_basebackup` stops its
  WAL-streaming child only from an `atexit` handler, which a signal does not run.
- Because the command is in another group, the postmaster's signals to our group do not
  reach it — hence `standby_command_quickdie()` and `standby_command_atexit()`.
- `pg_ctl start` puts the standby in **its own session**, so the standby is not killed
  along with `pg_ctl`.

### Failure messages

On a non-zero exit, `command_output_excerpt()` reads the end of the log file (at most
64 KB, only what this command wrote) and picks:

- the last 5 error lines (`error:`, `ERROR:`, `FATAL:`, `PANIC:`) with their
  `DETAIL`/`HINT` lines;
- for a `FATAL`/`PANIC`, also the lines just before it **from the same PID** (from
  `[pid]` in `log_line_prefix`), because a server often logs the real cause first and
  the `FATAL` only sums it up;
- with no error lines, just the last 5 lines.

That excerpt goes into the `WARNING`'s `DETAIL`, so the reason shows in the **primary's**
log, not only in the standby log.

### Shell safety

- Every argument goes through `shell_quote()` (single quotes, `'` → `'\''`).
- `pg_ctl` itself starts postgres through `/bin/sh` with the data directory and log path
  in **double** quotes, where `$`, `` ` ``, `"` and `\` are still active, and
  `shell_quote()` cannot help there. So any path containing those characters is refused:
  `path_shell_unsafe_char()` is checked in `resolve_standby_directory()`, in
  `run_standby_tick()` for the log file, and in `check_dbblue_standby_directory()`.

### Binaries

`resolve_bin_path()` takes them from the same directory as the running `postgres`
(`my_exec_path`), so a standby is always built and run by the same version.

---

## 16. Worker state (static variables)

All of this lives in the worker process only and is lost when it restarts. Anything that
must survive a restart is on disk (the fence in the standby's `postgresql.auto.conf`, the
directory in `dbblue_standby.dir`, the build marker).

| Variable | Meaning | Reset by |
|---|---|---|
| `consecutive_failures` | failed attempts in a row | success, reload |
| `retry_at` | no attempt before this time | success, reload |
| `standby_mode_applied` | mode last acted on (`-1` = none yet) | main loop |
| `slot_wal_limit_warned` | `max_slot_wal_keep_size = -1` warning given | reload |
| `sync_standby_warned` | `synchronous_standby_names` check done | reload |
| `space_recheck_at`, `space_short_warned` | disk-space recheck time / warned | reload, enough space |
| `tablespace_warned` | tablespace warning given | reload, no tablespaces |
| `standby_last_apply`, `standby_last_flush` | LSNs seen last tick | every start |
| `standby_stalled_since` | when it stopped making progress, or 0 | progress, every start |
| `standby_lagging_ticks` | ticks in a row over `max_lag` | not lagging, every start |
| `standby_running_fenced` | is the running standby fenced | each running tick, start, set aside |
| `standby_fence_pending` | fence to apply on the next start after a failed stop (`-1` none) | start, set aside |
| `slot_idle_since` | slot has had no streamer since | slot not idle |
| `standby_reload_pending` | primary reloaded, standby not yet | `reload_standby()` success, start |
| `command_pgid` | process group of the running command, or 0 | end of `run_command()` |

---

## 17. Files on disk

| Path | Written by | Read by | Notes |
|---|---|---|---|
| `<DataDir>_standby/` (or `dbblue_standby_directory`) | `pg_basebackup` | everything | the standby's data directory |
| `<dir>/postgresql.auto.conf` | `pg_basebackup -R`, then `write_standby_settings()` | `standby_conf_value()` | port, fence, limits, conninfo, slot name |
| `<dir>/standby.signal` | `pg_basebackup -R` | `standby_identity()` | missing = promoted |
| `<dir>/postmaster.pid` | the standby | `standby_pid_state()`, `standby_pidfile_port()`, `reload_standby()`, `stop_standby_at_shutdown()` | removed by us only when surely stale |
| `<dir>.building` | `mark_standby_build()` | `standby_build_marker_owner()` | contains the owner `DataDir`; fsynced |
| `<dir>.pid` | the standby | — | only if the primary sets `external_pid_file` |
| `<DataDir>/dbblue_standby.port` | `report_standby_port()` | Odoo tooling, `reported_standby_port()` | exists **only** while the standby runs open; `.tmp` + `durable_rename()` |
| `<DataDir>/dbblue_standby.dir` | `retire_previous_standby()` | `previous_standby_directory()` | absolute path of the managed directory |
| `<DataDir>/dbblue_standby.slot` | `record_standby_slot_owner()`, `adopt_legacy_slot_owner()` | `standby_slot_owner()` | absolute path of the directory the slot was made for (§10) |
| `<primary log>-standby.log` or `<dir>.log` | `run_command()` (`>>`), `pg_ctl -l` | humans, `command_output_excerpt()` | chosen by `standby_log_path()` |

**`standby_log_path()`:** if the primary's stderr (`/proc/self/fd/2`) is a regular file
whose directory we can write to (as with `pg_ctl -l` or Debian's `pg_ctlcluster`), the
standby log goes next to it as `<log without .log>-standby.log`, so the same logrotate
rule covers it. Otherwise it is `<dir>.log`, which nothing rotates.

**Excluded from copies:** `dbblue_standby.port`, `dbblue_standby.dir`, `dbblue_standby.slot` and their `.tmp`
files are in the exclude lists of `basebackup.c` and `pg_rewind/filemap.c`. A copy of
this server must not inherit them: they would name a standby the copy does not have.

---

## 18. Settings and check hooks

All four settings are `PGC_SIGHUP`. A reload is enough.

| Setting | Type | Default | Check hook |
|---|---|---|---|
| `dbblue_create_standby` | enum `off`/`paused`/`on` | `off` | — |
| `dbblue_standby_directory` | string | `''` | `check_dbblue_standby_directory()` |
| `dbblue_standby_max_lag` | int, MB | `64` (0 = no lag fence) | — |
| `dbblue_standby_user` | string | `''` (= OS user) | `check_dbblue_standby_user()` |

### Check hooks: strict only inside a transaction

Both hooks do the **full** check only when `IsTransactionState()` is true and we are not
`InitializingParallelWorker` — in practice, for `ALTER SYSTEM`. Reading the config file
at startup or reload is **not** checked strictly, because:

- a server must never refuse to start over this setting;
- the operator may have removed the directory on purpose, to get a rebuild;
- every copy of the cluster, the standby included, carries the value;
- a parallel worker restores the leader's value inside a transaction, and the directory
  may have changed since (being rebuilt, half-filled by a build).

`run_standby_tick()` repeats the checks that matter at build time.

`check_dbblue_standby_directory()`:

- **always:** canonicalize the path (so a trailing `/` does not put `<dir>.log` inside
  the directory); refuse `$`, `` ` ``, `"`, `\`;
- **in a transaction:** not too long; not inside `DataDir` (symlinks resolved, see
  `path_inside_data_dir()`); exists; is a directory; owned by the server's OS user;
  writable; and either empty or a cluster whose `primary_slot_name` is
  `dbblue_standby`.

`check_dbblue_standby_user()`:

- **always:** length < `NAMEDATALEN`;
- **in a transaction with a database:** the role exists, can log in, and has
  `REPLICATION` or `SUPERUSER`.

`standby_directory_path()` resolves the value: empty → `<DataDir>_standby`; relative →
relative to the **parent** of `DataDir`; absolute → as is.

---

## 19. Constants

All at the top of `dbblue_create_standby.c`.

| Constant | Value | Used for |
|---|---|---|
| `DBBLUE_STANDBY_NAPTIME_MS` | 10 s | tick interval |
| `DBBLUE_STANDBY_PORT_ATTEMPTS` | 10 | port range above the primary's |
| `DBBLUE_STANDBY_QUICK_RETRIES` | 5 | failures retried every tick |
| `DBBLUE_STANDBY_BACKOFF_MIN_MS` / `_MAX_MS` | 1 min / 60 min | backoff range |
| `DBBLUE_STANDBY_SPACE_RECHECK_MS` | 60 s | disk-space recheck |
| `DBBLUE_STANDBY_COMMAND_POLL_MS` | 100 ms | `run_command()` poll |
| `DBBLUE_STANDBY_COMMAND_KILL_MS` | 5 s | `SIGTERM` → `SIGKILL` |
| `DBBLUE_STANDBY_STALL_MS` | 60 s | stall/disconnect fence; idle-slot xmin release |
| `DBBLUE_STANDBY_LAG_TICKS` | 2 | ticks over `max_lag` before fencing |
| `DBBLUE_STANDBY_SLOT_RELEASE_MS` | 5 s | wait for slot release before drop |
| `DBBLUE_STANDBY_STOP_WAIT_MS` | 60 s | shutdown wait for the standby |
| `DBBLUE_STANDBY_OUTPUT_LINES` / `_LINE_MAX` / `_SCAN` | 5 / 512 B / 64 KB | failure excerpt |
| `DBBLUE_STANDBY_SLOT` | `"dbblue_standby"` | slot name |
| `DBBLUE_STANDBY_NAME` | `"dbblue_standby"` | `cluster_name` / `application_name` |
| `DBBLUE_STANDBY_DIR_FILE` | `"dbblue_standby.dir"` | directory memory file |
| `DBBLUE_STANDBY_SLOT_FILE` | `"dbblue_standby.slot"` | which directory the slot was made for |
| `DBBLUE_DEBIAN_CONF_DIR` | `"/etc/postgresql"` | claimed-port scan (overridable at compile time) |

---

## 20. Rules every change must keep

These are the lessons behind most of the `[FIX]` commits on this file
(`git log --oneline -- src/backend/postmaster/dbblue_create_standby.c`). Breaking one
usually brings an old bug back.

1. **Never delete or stop what you cannot prove is ours.** Use `standby_identity()` and
   check for `STANDBY_OURS`. Promoted, foreign, other-primary and hand-built standbys are
   only warned about. Only `rmtree(dir, false)` (contents, not the directory itself).
2. **Never `ERROR`.** Report a `WARNING` and return `false`. An `ERROR` costs the worker
   for 60 s.
3. **Never block.** Run commands through `run_command()`, never `system()`. Any new
   waiting loop must check `ShutdownRequestPending` (and if it is long, `ConfigReloadPending`).
4. **Start nothing once shutdown is requested.** The postmaster waits for us to exit.
   Leave cleanup to the next start (as with partial copies and slot drops).
5. **Port file means "send reads here".** Call `forget_standby_port()` before anything
   that makes the standby unusable: fence, stop, restart, identity problems, mode change.
6. **Write files atomically and durably.** `.tmp` + `durable_rename()`; fsync markers.
7. **Quote every shell argument** with `shell_quote()`, and refuse paths containing
   `$`, a backtick, `"` or `\` wherever `pg_ctl` will put them in double quotes.
8. **Keep the port stable.** Prefer the last port; never take a port another cluster is
   configured for.
9. **Never drop an active slot**, and never build without a `max_slot_wal_keep_size`
   limit.
10. **Never act in recovery.** The standby shares our config.
11. **Free what you allocate.** No memory context is reset between ticks.
12. **Warn once, not every 10 s.** Use a `*_warned` flag that `apply_config_reload()`
    clears.

---

## 21. Testing

The TAP test is
[src/test/modules/dbblue_standby/t/001_auto_standby.pl](../src/test/modules/dbblue_standby/t/001_auto_standby.pl).
It needs a build configured with TAP tests, and takes a few minutes (ticks are 10 s, the
stall fence 60 s).

```bash
# meson
meson test -C build --suite dbblue_standby --print-errorlogs

# make (configure --enable-tap-tests)
make -C src/test/modules/dbblue_standby check
```

The `Makefile` installs `pg_stat_statements`, `hypopg` and `pg_tde` into the temp
install, because this fork's default `shared_preload_libraries` names them.

### What it covers

| Section | Behaviour |
|---|---|
| The build | standby built in `<datadir>_standby`, port reported, data replicates |
| M4 | a port that a **stopped** cluster next to ours is configured for is skipped |
| L5 | `hot_standby = off` in the shared `postgresql.conf` does not lock the standby |
| M2 | a primary reload reaches the standby without restarting it |
| H1 | the standby stops with the primary and keeps its port |
| H3, H4 | a standby whose replay is paused is fenced (port file gone, refuses connections, slot xmin cleared), then let back in |
| Stalled lag | a standby whose replay is paused and that falls more than `dbblue_standby_max_lag` behind is fenced for its lag within a couple of ticks, not by the 60 s stall timer |
| Disconnected lag | a standby shut out by `pg_hba.conf` that falls more than `dbblue_standby_max_lag` behind its slot's `restart_lsn` is fenced within a couple of ticks; let in again, it catches up and is unfenced |
| L8 | an `idle_replication_slot_timeout` invalidation is reported as such, and the standby rebuilt |
| M6 | with `max_slot_wal_keep_size = -1`, a standby whose slot is gone is kept, then rebuilt once a limit is set |
| Slot owner | pointed back at a directory whose standby was set aside for one built in another, that standby is rebuilt, never started on its old data or left fenced |
| H4 | a promoted standby is no longer treated as ours, and its slot is dropped |

### What it does not cover (test by hand)

- cancelling a build in progress (M3) — needs a database big enough that the copy takes
  a while; set the mode to `paused` during the copy and look for `stopping pg_basebackup`;
- leaving a partial copy on shutdown (L7) — stop the primary during a build, start it
  again, look for `clearing … left behind by a build that was interrupted`;
- the retry backoff (M5) — make the parent directory unwritable and watch the
  `consecutive failures` messages;
- the `pg_upgrade` guard (L1);
- a port taken at a non-loopback listen address (L6).

### Manual smoke test

```bash
initdb -D /tmp/p && pg_ctl -D /tmp/p -l /tmp/p.log start
psql -c "ALTER SYSTEM SET dbblue_create_standby = on" -c "SELECT pg_reload_conf()"
tail -f /tmp/p.log /tmp/p-standby.log       # build, then "standby is running on port …"
cat /tmp/p/dbblue_standby.port
psql -p "$(cat /tmp/p/dbblue_standby.port)" -c "SELECT pg_is_in_recovery()"
```

To force a **fence**: on the standby run `SELECT pg_wal_replay_pause();`, write on the
primary, wait about 70 s.

---

## 22. How to make common changes

### Add a setting the standby must get

1. In `write_standby_settings()`, add the key to the list of dropped lines
   (`conf_line_sets(line, "<name>")`), so it does not pile up.
2. Append the new value after the others. Use `append_conf_string()` for strings.
3. Explain in the function's header comment **why** the standby needs it.
4. If it needs a restart to take effect, decide whether a running standby must be
   restarted (see `standby_limits_behind()` for the pattern).

### Add a new "must be at least as high as the primary" limit

Add the name to `standby_limit_names[]` **and** fill it in `standby_limit_values()`, in
the same order, from both the live value and `ControlFileData`.

### Add a new precondition for the build

Put it in step 10 of `run_standby_tick()`. Decide:

- the operator must fix it, and retrying cannot help soon → warn **once** (flag cleared
  in `apply_config_reload()`) and return `true`, as `standby_copyable()` does;
- it may clear up by itself → warn and return `false`, so the backoff applies.

### Add a new reason to fence

Add it to `standby_fence_changes()`. Return `true` only when the fence must **flip**.
The caller handles `forget_standby_port()`, `standby_fence_pending` and the restart.

### Add a new log/warning

Start the message with `"dbblue create standby: "` — operators grep for it, and
[AUTO_STANDBY.md](AUTO_STANDBY.md) tells them to alert on it. Add a `HINT` that says what
to do. If the operator should react to it, add it to the "Log lines to alert on" table in
the operator guide.

### Add a new file in the primary's data directory

Add it (and its `.tmp`) to the exclude lists in
[basebackup.c](../src/backend/backup/basebackup.c) and
[pg_rewind/filemap.c](../src/bin/pg_rewind/filemap.c). Otherwise every copy of the
server, including the standby itself, inherits it.

### After any change

- run the TAP test;
- update [AUTO_STANDBY.md](AUTO_STANDBY.md) if operators see a difference;
- update this page if the flow, state or rules changed.

---

## 23. From a log line to the code

Every message starts with `dbblue create standby:`. Search the source for the rest of the
text. The most common ones:

| Message contains | Function |
|---|---|
| `worker started` / `worker shutting down` | `DbblueCreateStandbyMain()` |
| `creating standby in` | `run_standby_tick()` (build) |
| `standby is running on port` | `report_standby_port()` / `run_standby_tick()` |
| `standby moves from port` | `run_standby_tick()` (port choice) |
| `no free port found in range` | `run_standby_tick()` / `find_free_port()` |
| `port %d is free on 127.0.0.1, but taken at listen address` | `port_is_free()` |
| `another server holds its Unix socket` | `port_is_free()` / `socket_port_in_use()` |
| `more than dbblue_standby_max_lag, restarting it to refuse connections` | `standby_fence_changes()` |
| `has not streamed` / `has not replayed past` | `standby_fence_changes()` |
| `starting it to refuse connections` | `run_standby_tick()` (start-time fence) |
| `has caught up with this server` | `standby_fence_changes()` |
| `no longer holds back VACUUM` | `release_idle_slot_xmin()` |
| `restarting the standby … to set its` / `which runs with` | `standby_limits_behind()` |
| `raising the standby's` | `write_standby_settings()` |
| `reloading the configuration of the standby` | `reload_standby()` |
| `has been invalidated, rebuilding` / `does not exist, rebuilding` | `vet_existing_standby()` |
| `is no longer a standby of this server` | `vet_existing_standby()` (`PROMOTED`) |
| `holds a different database cluster` | `vet_existing_standby()` (`FOREIGN`) |
| `is a standby of another server` | `vet_existing_standby()` (`OTHER_PRIMARY`) |
| `did not build, and is left alone` | `vet_existing_standby()` (`NOT_BUILT`) |
| `was made for the standby in …, not for this one, rebuilding` / `which still streams through it` | `vet_existing_standby()` (slot owner, §10) |
| `is being built, or was left half built, by the server with data directory` | `run_standby_tick()` (marker owner) |
| `clearing … left behind by a build that was interrupted` | `run_standby_tick()` |
| `leaving the partial copy` | `run_standby_tick()` (build failed during shutdown) |
| `not enough free disk space` | `standby_has_room()` |
| `cannot build a standby of this server, as it has tablespaces` | `standby_copyable()` |
| `not building a standby` / `not rebuilding the standby` | `run_standby_tick()` / `vet_existing_standby()` |
| `max_slot_wal_keep_size is -1, so replication slot` | `warn_unlimited_slot_wal()` |
| `makes the standby of dbblue_create_standby a synchronous standby` | `warn_synchronous_standby()` |
| `stopping %s, as dbblue_create_standby is now` / `as dbblue_standby_directory no longer names` | `run_command()` (build cancelled) |
| `%s failed:` | `run_command()` |
| `exited during startup` / `is still starting` | `run_standby_tick()` (after `pg_ctl start`) |
| `removing stale` / `cannot tell whether the process` | `run_standby_tick()` (stale `postmaster.pid`) |
| `stopped the standby in …, as dbblue_create_standby is` | `set_standby_aside()` |
| `stopped the standby in …, as dbblue_standby_directory now names` | `retire_previous_standby()` |
| `shutting down the standby in …, as this worker is stopping` | `stop_standby_at_shutdown()` |
| `consecutive failures; retrying in` | `DbblueCreateStandbyMain()` |
| `dropping replication slot` / `is in use by another process` | `drop_standby_slot()` |
| `cannot use standby directory` | `standby_parent_usable()` |
