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
initializes a cluster in `/var/lib/dbblue/19/data`, and starts the `dbblue`
service on `localhost:5432`.

## What's static vs generated

| Field | Where it comes from |
|-------|---------------------|
| name, description, maintainer | `templates/control` (edit by hand) |
| version | `VERSION` env / default in `build-deb.sh` |
| **`Depends:`** | **generated per build** by `dpkg-shlibdeps` — never hand-edit |

## After install — management

The package installs two convenience commands (on the system `PATH`):

```bash
sudo dbblue-status     # pg_lsclusters-style line: Ver / Cluster / Port / Status / Data dir
sudo dbblue-psql       # connect as the dbblue superuser on whatever port it landed on
```

**Ports:** the install auto-picks the first free port from 5432 up (so it never
clashes with an existing PostgreSQL); `dbblue-status` shows which one.

**Logs:** file logs at `/var/log/dbblue/postgresql-YYYY-MM-DD.log`, and everything
also goes to the systemd journal (`journalctl -u dbblue`).

## Files

- `build-deb.sh` — the packager (staging → `.deb`)
- `templates/control` — package metadata (`@VERSION@`/`@ARCH@`/`@DEPS@` filled in)
- `templates/{postinst,prerm,postrm}` — install/remove scripts (user, initdb, service, port, logs)
- `templates/systemd/dbblue.service` — the service unit
- `templates/profile.d/dbblue.sh` — puts client tools on `PATH`
- `templates/bin/{dbblue-status,dbblue-psql}` — management helper commands
