# Packaging & Distribution

How DBblue is packaged as a Debian package and shipped to users through a
signed APT repository — the same model as the upstream PostgreSQL (PGDG) repos.

- **Package:** `dbblue-postgres`
- **APT repo:** <https://cybrosystech.github.io/dbblue-apt> (source: `cybrosystech/dbblue-apt`)
- **Signing identity:** `DBblue Packages <dbblue@cybrosys.info>`
- **Supported releases:** Ubuntu 22.04 (`jammy`), Ubuntu 24.04 (`noble`)

Detailed docs live next to each part:
- [`packaging/deb/README.md`](packaging/deb/README.md) — building the `.deb`
- [`packaging/apt/README.md`](packaging/apt/README.md) — building & hosting the repo

---

## What the package installs

| | Path |
|---|---|
| Binaries | `/opt/dbblue/19/bin` |
| Cluster data | `/var/lib/dbblue/clusters/<name>` |
| Config | `/var/lib/dbblue/clusters/<name>/postgresql.conf` |
| Logs | `/var/log/dbblue/<name>/` (also the systemd journal) |
| Service | `dbblue@<name>.service` (one per cluster) |
| Commands | `dbblue-createcluster`, `dbblue-dropcluster`, `dbblue-lsclusters`, `dbblue-status`, `dbblue-psql` |

On install the package creates a default cluster **`main`**, auto-selects a free
port (5432 up, so it never clashes with an existing PostgreSQL), enables file
logging, and starts the service. Runtime dependencies — including `liblz4` —
are declared automatically, so `apt` pulls them in.

DBblue installs standalone under `/opt` and is **not** managed by
`postgresql-common` (its version 19 would collide with a system PostgreSQL 19),
so it uses its own `dbblue-*` commands instead of `pg_ctlcluster`/`pg_lsclusters`.

---

## Prerequisites (build host)

```bash
sudo apt-get install -y build-essential pkg-config bison flex perl \
     libreadline-dev zlib1g-dev libicu-dev liblz4-dev dpkg-dev fakeroot
```
`liblz4-dev` is required — this fork defaults `wal_compression` to lz4, so a
build without LZ4 would fail at `initdb`. `--with-lz4` therefore defaults to on.

---

## Cutting a release

**The version lives in one file:** [`packaging/deb/VERSION`](packaging/deb/VERSION).
The git tag is only a build trigger.

### Automated (recommended) — GitHub Actions

The workflow [`.github/workflows/release-deb.yml`](.github/workflows/release-deb.yml)
builds the `.deb` on Ubuntu 22.04 **and** 24.04 runners, signs each into its
codename pocket, and publishes to the APT repo.

```bash
# bump only when real users already have a prior build installed:
echo "19~beta2-2" > packaging/deb/VERSION
git commit -am "release 19~beta2-2"
git push origin staging_dbblue

# trigger the build (a tag must be unique each time):
git tag v19beta2-2 && git push origin v19beta2-2
```

One-time CI setup (repo → Settings → Secrets and variables → Actions):
- `DBBLUE_GPG_PRIVATE_KEY` — `gpg --armor --export-secret-keys dbblue@cybrosys.info` (passphraseless)
- `DBBLUE_APT_TOKEN` — a token with write access to `cybrosystech/dbblue-apt`

### Manual — run on the target Ubuntu release

```bash
rm -rf stage dist
make clean && ./configure --prefix=/opt/dbblue/19 --with-lz4 CFLAGS="-O2"
make -j"$(nproc)"
make install            DESTDIR="$PWD/stage"
make -C contrib install DESTDIR="$PWD/stage"
packaging/deb/build-deb.sh                                   # -> dist/*.deb
GPG_KEY=dbblue@cybrosys.info \
  CODENAME=$(. /etc/os-release; echo "$VERSION_CODENAME") \
  packaging/apt/build-repo.sh dist/*.deb                     # -> signed dbblue-apt/
cd dbblue-apt && git add -A && git commit -m "release" && git push
```

**Versioning rule:** the package version must increase for `apt upgrade` to
reach installed users. Bump the revision (`-2`, `-3`) for packaging-only
changes; bump the upstream part (`19~beta3`) when the database code changes.
While there are no users, the version can stay the same across rebuilds.

---

## Supporting a new Ubuntu release

Build **one `.deb` per release** — dependencies (`libicu`, `libreadline`, …)
are recomputed per OS automatically. To add one (e.g. 26.04), add it to the CI
matrix in `release-deb.yml`:

```yaml
- os: ubuntu-26.04
  codename: <codename>
```
then cut a release as above. No VMs or containers required.

---

## End-user installation

```bash
curl -fsSL https://cybrosystech.github.io/dbblue-apt/setup-dbblue-repo.sh | sudo sh
sudo apt install -y dbblue-postgres
```
Works identically on every supported Ubuntu — the setup script detects the OS
codename and `apt` pulls the matching build. Updates: `sudo apt update && sudo apt upgrade`.

---

## Directory map

```
PACKAGING.md                       # this file — overview + release runbook
packaging/
├── deb/
│   ├── build-deb.sh               # staged install -> signed-off .deb
│   ├── VERSION                    # the package version (edit to bump)
│   ├── templates/control          # package metadata
│   ├── templates/postinst,prerm,postrm
│   ├── templates/systemd/dbblue@.service
│   ├── templates/bin/dbblue-*     # cluster management commands
│   └── README.md
└── apt/
    ├── build-repo.sh              # .deb(s) -> signed APT repo
    ├── setup-dbblue-repo.sh       # the end user's one-time "add repo" script
    └── README.md
.github/workflows/release-deb.yml  # CI: build (jammy+noble) -> sign -> publish
```
