# DBblue — Auto Standby: Operator Guide

With one setting, DBblue builds a **streaming read replica of your database on the
same server**, keeps it running, and tells you the port to give Odoo. Odoo then sends
its read-only work there: list views, `read`/`search_read` calls and reports. The
primary is left with less to do.

```sql
ALTER SYSTEM SET dbblue_create_standby = on;
SELECT pg_reload_conf();
-- the server log then says:
-- LOG:  dbblue create standby: standby is running on port 5433 (data directory ".../main_standby")
-- HINT: For Odoo, set db_replica_port = 5433, and db_replica_host ...
```

> **What it is — and is not**
> - It **is** a read replica for offloading Odoo's reads, and a warm local copy of the data.
> - It is **not** high availability or disaster recovery. The standby shares the
>   primary's host, disks, kernel and power supply, so it does not survive their failure.
>   For real HA, run a replica on **another** machine.
> - It is **off by default**, and changes nothing until you turn it on.

Developers changing the feature: see [AUTO_STANDBY_INTERNALS.md](AUTO_STANDBY_INTERNALS.md)
for how the worker works inside.

---

## Quick reference

| Setting | Default | What it does |
|---|---|---|
| `dbblue_create_standby` | `off` | `on`: build, run and watch the standby. `paused`: stop it but keep its slot and files. `off`: stop it and drop its slot. |
| `dbblue_standby_directory` | `''` | Where the standby lives. Empty means `<data directory>_standby`, beside the primary's. A relative path is taken from the data directory's parent. |
| `dbblue_standby_max_lag` | `64MB` | How far behind the standby may be and still accept connections. Further behind, it refuses them until it has caught up. `0` turns the check off. |
| `dbblue_standby_user` | `''` | Role `pg_basebackup` connects as. Empty means the operating-system user the server runs as. |

All four take effect on a reload (`SELECT pg_reload_conf();`); no restart is needed.

---

## How it works

A background worker in the primary does everything. Every 10 seconds it checks on
the standby.

1. **Build.** If the standby directory holds no standby yet, it copies the primary
   with `pg_basebackup`, streaming through a replication slot named `dbblue_standby`.
   With pg_tde WAL encryption on, the standby's WAL is encrypted as well.
2. **Start.** It starts the standby on a free port among the ten above the primary's.
   It keeps the same port across restarts whenever it can, because Odoo's
   `db_replica_port` is set by hand.
3. **Report.** It logs the port, and writes it to `<data directory>/dbblue_standby.port`.
4. **Watch.**
   - A standby that crashed is started again.
   - A standby too far behind is **fenced**: restarted so that it refuses connections,
     while it keeps replaying (see below).
   - A standby whose slot is lost is rebuilt.
5. **Stop.** When the primary stops, the standby is stopped cleanly first. When the
   primary starts, the standby is started again.

The standby runs on the primary's own `postgresql.conf`, `pg_hba.conf` and
`pg_ident.conf`. Its own `postgresql.auto.conf` overrides only what must differ:
- the port, and `cluster_name = 'dbblue_standby'`
- half of `shared_buffers`, `work_mem` and `effective_cache_size`
- the connection limits a standby needs, never lower than the primary's
- `hot_standby` and `hot_standby_feedback`

When you reload the primary, the standby reloads too.

### Fencing: Odoo never reads stale data

A standby that is behind still answers queries, but with old data. So the worker
**fences** it: it restarts the standby with `hot_standby = off`. A fenced standby keeps
streaming and replaying, but refuses every connection with
`FATAL: the database system is not accepting connections`. Odoo cannot connect, and
reads from the primary instead. Once the standby has caught up, the worker restarts it
open again.

The worker fences the standby when any of these happens:

| Situation | Trigger |
|---|---|
| Starting with a backlog (after a pause, a crash or a reboot) | More than `dbblue_standby_max_lag` of WAL to catch up on |
| Running, but falling behind (heavy batch, slow disk), replay stopped (waiting on a conflicting query, paused), or not streaming at all (shut out by `pg_hba.conf`, say) | More than `dbblue_standby_max_lag` behind on 2 checks in a row |
| Replay stopped, or not streaming at all, even if not far behind | 60 seconds without progress |

While fenced, the port file is removed and the slot no longer holds back VACUUM on the
primary. Each fence and unfence is a restart, so it drops open replica connections.
Odoo retries the replica about 20 minutes after a failed connection.

---

## Before you turn it on

**Disk.**
- The standby is a full copy: plan for **2× the database**, on the same filesystem by default.
- On top of that, the primary keeps up to `max_slot_wal_keep_size` of WAL for the standby
  while it is down or behind.
- Before every build the worker checks that the copy fits and leaves a margin. If it
  does not, it warns once and waits.

**Memory.**
- The standby takes **half** of the primary's `shared_buffers`, `work_mem` and
  `effective_cache_size`, on top of what the primary uses.
- With `huge_pages = on`, size `vm.nr_hugepages` for both servers, or the standby
  cannot start.

**CPU and I/O.**
- Replay competes with the primary for the same disks.
- Every build or rebuild is a full read of the database, plus an immediate checkpoint
  (`pg_basebackup -c fast`). Prefer turning it on outside business hours.

**Settings that must be right.**

| Setting | Needs | Why |
|---|---|---|
| `wal_level` | `replica` or higher (the default) | Streaming replication |
| `max_wal_senders` | at least 1 free | The standby's connection |
| `max_slot_wal_keep_size` | **not** `-1` (auto-tune sets 2× `max_wal_size`) | Without a limit, a stopped standby could fill the disk with WAL. The worker refuses to build one. |
| `pg_hba.conf` | a `replication` entry that needs no typed password: `local replication all peer` or `trust`, or a password from a `.pgpass` file | The worker connects over the first Unix socket directory, or `localhost`, with `pg_basebackup -w` |
| `synchronous_standby_names` | empty (the default), or a list of **names** — never `'*'` | `'*'` makes this standby synchronous, and commits hang while it is down (see Troubleshooting) |
| Tablespaces | **none** | On the same host the standby would use the primary's own tablespace directories and overwrite its files. The worker will not build a standby while a tablespace exists, and `CREATE TABLESPACE` is refused while the feature is `on` or `paused` |

**Security.** The standby listens on the primary's `listen_addresses`, on a port up to
ten above the primary's. With `listen_addresses = '*'`, make sure your firewall covers
that port range too.

---

## Turning it on

```sql
ALTER SYSTEM SET dbblue_create_standby = on;
SELECT pg_reload_conf();
```

Then watch the server log:

```
LOG:  dbblue create standby: creating standby in "/var/lib/.../main_standby" from primary on port 5432
LOG:  dbblue create standby: standby is running on port 5433 (data directory "/var/lib/.../main_standby")
HINT: For Odoo, set db_replica_port = 5433, and db_replica_host the way db_host is set: ...
```

The build takes as long as copying the database once. To put the standby somewhere
else, set `dbblue_standby_directory` first. The directory must:
- already exist, owned by the server's operating-system user
- be empty
- lie outside the data directory, symbolic links included

---

## Connecting Odoo

In `odoo.conf`, next to the primary's `db_host` and `db_port`:

```ini
db_replica_host = localhost        ; or the Unix socket directory, as for db_host
db_replica_port = 5433             ; the port from the log, or from dbblue_standby.port
```

**`db_replica_host` must be set.** Odoo only uses a replica when it is, even for a local
socket.

What happens in Odoo:
- **Reads:** read-only calls (`web_search_read`, `web_read`, read-only reports) go to the
  replica.
- **Writes** always go to the primary.
- **Fallback:** if the replica refuses a connection (fenced, restarting, down), Odoo
  falls back to the primary and tries the replica again about **20 minutes** later.
  Users see no error.

**Read-after-write.** A replica is always a moment behind. Within
`dbblue_standby_max_lag` (64 MB by default), a record just saved may not appear on a
list view refresh for a short while. Lower `dbblue_standby_max_lag` if that matters.

**If the port changes**, the worker warns:

```
WARNING: dbblue create standby: standby moves from port 5433 to port 5434, as port 5433 is taken or configured for another cluster
HINT:    Set Odoo's db_replica_port to 5434.
```

Update `db_replica_port` and restart Odoo. Until then, Odoo falls back to the primary.

---

## Modes: on, paused, off

| Mode | Standby | Slot | Files | Turning it `on` again |
|---|---|---|---|---|
| `on` | running, watched | kept | kept | — |
| `paused` | stopped | **kept**: WAL kept up to `max_slot_wal_keep_size`; VACUUM no longer held back after a minute | kept | resumes streaming. Starts fenced if more than `dbblue_standby_max_lag` behind; rebuilt if the slot was invalidated meanwhile |
| `off` | stopped | **dropped**: nothing held back on the primary | kept | rebuilt from scratch |

Use `paused` for short maintenance windows, and `off` to stop for good or to save the
WAL.

---

## What the worker handles on its own

| Event | What happens |
|---|---|
| Standby crashes | Restarted within about 10 s, same port |
| Primary restarts | Standby stopped first, started again with the primary, same port |
| Primary crashes or immediate stop | Standby keeps running; picked up again when the primary starts |
| Standby far behind, or replay stopped | Fenced until caught up (see above) |
| Slot invalidated, or dropped | Standby rebuilt; the log says why (`wal_removed` vs `idle_replication_slot_timeout`) |
| `dbblue_standby_directory` pointed back at a directory whose standby was set aside for one built elsewhere | That standby is rebuilt, never started on its old data: the build elsewhere dropped the slot it streamed through |
| A start or build keeps failing | Retried 5 times 10 s apart, then after 1, 2, 4 … minutes, up to once an hour; `SELECT pg_reload_conf()` retries at once |
| Primary stopped during a build | The partial copy is cleared at the next start, not during the shutdown |
| Primary settings raised (`max_connections` and the like) | The standby's are raised to match, and it is restarted if needed |
| The worker itself sent SIGTERM | Standby stopped; the worker comes back within a minute and starts it again |
| `pg_upgrade` | The worker does not run during the upgrade; the new cluster builds its own standby at its first start |

---

## Monitoring

**Is the standby streaming, and how far behind?**

```sql
SELECT application_name, state, sync_state,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(), replay_lsn)) AS replay_lag,
       replay_lag AS replay_lag_time
FROM pg_stat_replication
WHERE application_name = 'dbblue_standby';
```

Expect one row, `streaming` and `async`. No row means the standby is down, fenced while
reconnecting, building, or paused. A standby started by an older DBblue version streams
as `walreceiver` (or the primary's `cluster_name`) until its next restart.

**Is the slot healthy, and how much WAL does it keep?**

```sql
SELECT active, wal_status, invalidation_reason,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_kept
FROM pg_replication_slots
WHERE slot_name = 'dbblue_standby';
```

How to read the result:
- `wal_status = 'lost'`, or an `invalidation_reason`: a rebuild is coming.
- `wal_kept` growing towards `max_slot_wal_keep_size`: the standby has been away too long.

**Is it open to Odoo?** `<data directory>/dbblue_standby.port` exists only while the
standby is running and **not** fenced.

**Log lines to alert on.** Every message starts with `dbblue create standby:`. The
important ones:

| Message contains | Meaning |
|---|---|
| `restarting it to refuse connections` / `starting it to refuse connections` | Fenced: Odoo reads from the primary meanwhile |
| `consecutive failures; retrying in` | Something keeps failing; the message before it says what |
| `has been invalidated, rebuilding` / `does not exist, rebuilding` / `not for this one, rebuilding` | Full rebuild under way |
| `not enough free disk space` / `cannot build a standby of this server` / `not building a standby` | No standby until you act (disk, tablespaces, `max_slot_wal_keep_size = -1`) |
| `standby moves from port` | Update Odoo's `db_replica_port` |
| `makes the standby of dbblue_create_standby a synchronous standby` | Fix `synchronous_standby_names` |
| `is a standby of another server` / `holds a different database cluster` / `did not build` | The directory holds something the worker will not touch; see Troubleshooting |

There is no SQL function for the worker's own state yet (building, fenced, backing
off). Use the log for that.

---

## Troubleshooting

**"synchronous_standby_names "*" makes the standby … a synchronous standby"**
Commits on the primary wait for the standby, and hang whenever it is paused, rebuilt or
restarted. A synchronous standby on the same host protects nothing anyway. Name your
real synchronous standbys instead:

```sql
ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (replica_dc2)';
SELECT pg_reload_conf();
```

**"… is a standby of another server, not of this one" after you changed the primary's
port or socket directory.** The standby still points at the old address. Do not
rebuild it. Do what the HINT says: edit `port=` (and `host=`) in `primary_conninfo` in
`<standby directory>/postgresql.auto.conf`, then run `SELECT pg_reload_conf();`. It
resumes through the existing slot.

**"… holds a different database cluster" or "… did not build"**
The directory holds a cluster the worker did not create: an old standby, a hand-made
one, or a promoted copy. The worker never deletes it. Remove it yourself if it is no
longer needed, or point `dbblue_standby_directory` at an empty directory.

**"not building a standby … as max_slot_wal_keep_size is -1"**
Set a limit, for example twice `max_wal_size`, and reload. An existing standby whose
slot is gone is kept, stopped, until then.

**The standby was promoted.** The worker stops reporting it and drops the slot, but
leaves it running, because it is now somebody's primary. **Remove or change Odoo's
`db_replica_port`**, or Odoo keeps reading from a copy that no longer follows the
primary.

**Long reports on the standby.** `hot_standby_feedback` is on, so a long query on the
standby holds back VACUUM on the primary for as long as it runs, just as it would on
the primary. In return, VACUUM on the primary does not cancel queries on the standby;
locks taken on the primary, such as `DROP TABLE` or `TRUNCATE`, still can.

**"cannot create a tablespace while dbblue_create_standby is …"**
A standby on the same host would share the new tablespace's directory with the primary
and overwrite its files, so `CREATE TABLESPACE` is refused while the feature is `on` or
`paused`. If you need the tablespace, set `dbblue_create_standby = off` first. The
standby is then not rebuilt for as long as a tablespace exists.

**Restarting the worker.** It has no database connection, so `pg_terminate_backend()`
cannot reach it. Use `kill <pid>` (SIGTERM). The standby stops, and both come back
within a minute. To retry a failure at once, `SELECT pg_reload_conf();` is enough.

---

## Cleanup after dropping or upgrading a cluster

Cluster tools (`pg_dropcluster`, `pg_upgradecluster`, `dbblue-dropcluster`) do not know
about the standby. Its directory is left behind on purpose, because the worker never
deletes data it cannot prove is its own.

- **After dropping a cluster:** delete `<data directory>_standby` and its log. It is a
  full, readable copy of the dropped database.
- **After a major-version upgrade:** the new cluster builds its own standby. Delete the
  **old** cluster's standby directory once you drop the old cluster.
- **With an explicit `dbblue_standby_directory`** carried into the upgraded cluster:
  empty it before starting the upgraded cluster. Otherwise the old standby is refused
  as "a different database cluster", and no new one is built.

---

## Files

| Path | What it is |
|---|---|
| `<data directory>_standby/` | The standby's data directory (default location) |
| `<data directory>_standby.building` | Marker while a build is in progress |
| `<data directory>/dbblue_standby.port` | The standby's port, present while it accepts connections |
| `<data directory>/dbblue_standby.dir` | Which directory holds the current standby |
| `<data directory>/dbblue_standby.slot` | Which directory the `dbblue_standby` slot was made for |
| `<primary log>-standby.log`, or `<data directory>_standby.log` | Output of `pg_basebackup`/`pg_ctl`, and the standby's log when it does not use the logging collector. The second location is not rotated. |
| `standby-<log_filename>` in an absolute `log_directory` | The standby's server log, with the logging collector on |

---

## Known limitations

- **Same host.** The standby is a read replica, not a failover target (see the top of
  this page).
- **No status function.** The worker's state (fenced, backing off, last error) is only
  in the log.
- **`ALTER SYSTEM` on the primary does not reach the standby.** The standby reads its
  own `postgresql.auto.conf`. Put settings both should have in `postgresql.conf`.
- **Containers.** Where the primary's postmaster is PID 1, as in the official Docker
  image, a standby crash can crash-restart the primary. Run containers with an init
  process (`docker run --init`).
- **Ports.** A stopped cluster is only recognised if it is under `/etc/postgresql` or
  beside the primary's data directory. Clusters elsewhere are seen only while they run.
- **Cluster tools.** Dropping or upgrading a cluster leaves the standby's files behind
  (see Cleanup).
