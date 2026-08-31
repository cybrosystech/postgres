# DBblue — Debian packaging

Builds the `dbblue-postgres` `.deb` for the OS release you run it on.
Build **once per Ubuntu/Debian release** (22.04, 24.04, …) — the dependency
list adjusts itself automatically each time.

## 1. Build tools (once per machine)

```bash
sudo apt update
sudo apt install -y build-essential pkg-config bison flex perl \
     libreadline-dev zlib1g-dev libicu-dev liblz4-dev dpkg-dev fakeroot
```

## 2. Build + package (run from the repo root)

```bash
make clean                                     # after any configure change
./configure --prefix=/opt/dbblue/19 --with-lz4 CFLAGS="-O2"
make -j"$(nproc)"
make install            DESTDIR="$PWD/stage"
make -C contrib install DESTDIR="$PWD/stage"

packaging/deb/build-deb.sh                     # -> dist/dbblue-postgres_<ver>_<arch>.deb
```

`build-deb.sh` generates the `Depends:` line automatically from the compiled
binaries (so `liblz4-1`, `libicu…` are correct for this release), and refuses
to build if `postgres` isn't actually linked against LZ4.

Override the version if needed: `VERSION=19~beta2-2 packaging/deb/build-deb.sh`.

## 3. Install (consumer)

```bash
sudo apt install ./dist/dbblue-postgres_*.deb
```

`apt` pulls in the runtime dependencies (including `liblz4-1`) automatically,
creates a default cluster named **`main`** in `/var/lib/dbblue/clusters/main`,
picks a free port (5432 up), and starts it as `dbblue@main.service`.

## What's static vs generated

| Field | Where it comes from |
|-------|---------------------|
| name, description, maintainer | `templates/control` (edit by hand) |
| version | `VERSION` env / default in `build-deb.sh` |
| **`Depends:`** | **generated per build** by `dpkg-shlibdeps` — never hand-edit |

## Managing clusters (like pg_createcluster)

DBblue runs one or more **named clusters**, each with its own port, log dir,
and systemd instance (`dbblue@<name>.service`). Commands (on the system `PATH`):

```bash
sudo dbblue-createcluster main          # initdb + free port + start (done automatically on install)
sudo dbblue-createcluster test 5440     # a second cluster on an explicit port
sudo dbblue-lsclusters                  # Ver / Cluster / Port / Status / Data dir  (= dbblue-status)
sudo dbblue-psql                        # connect to 'main' as the dbblue superuser
sudo dbblue-psql test -c '\l'           # connect to another cluster, pass args to psql
sudo dbblue-dropcluster test            # stop + permanently remove a cluster
```

Each cluster: data in `/var/lib/dbblue/clusters/<name>`, managed with
`systemctl {start,stop,restart} dbblue@<name>`.

**Ports:** `dbblue-createcluster` auto-picks the first free port from 5432 up
(so it never clashes with an existing PostgreSQL); `dbblue-lsclusters` shows it.

**Logs:** file logs at `/var/log/dbblue/<name>/postgresql-YYYY-MM-DD.log`, plus
the systemd journal (`journalctl -u dbblue@<name>`).

## Files

- `build-deb.sh` — the packager (staging → `.deb`)
- `templates/control` — package metadata (`@VERSION@`/`@ARCH@`/`@DEPS@` filled in)
- `templates/{postinst,prerm,postrm}` — install/remove scripts (user, default cluster, service)
- `templates/systemd/dbblue@.service` — the per-cluster service template
- `templates/profile.d/dbblue.sh` — puts client tools on `PATH`
- `templates/bin/dbblue-{createcluster,dropcluster,lsclusters,status,psql}` — cluster management commands
