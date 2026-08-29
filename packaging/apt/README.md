# DBblue — APT repository

Publishes the `dbblue-postgres` `.deb`s as a **signed APT repo**, so users
install with plain `apt` — the same model as the official PostgreSQL PGDG repo.

Two sides:

- **Producer** (you): create a signing key once, build the repo from your
  `.deb`s, host the folder.
- **Consumer** (Odoo user): add the repo once, then `apt install`.

---

## Producer — one-time: create a signing key

Do this once, on a machine you control, and keep the **private** key safe
(only the public key is published):

```bash
gpg --quick-generate-key "DBblue Packages <dbblue@cybrosys.info>" default default never
```

`apt` refuses unsigned repositories, so this key is what proves your packages
are authentic.

## Producer — build / refresh the repo (per OS release)

Build the `.deb` first (see `../deb/README.md`), then:

```bash
GPG_KEY=dbblue@cybrosys.info packaging/apt/build-repo.sh dist/*.deb
```

This creates/updates `./dbblue-apt/` with:

```
dbblue-apt/
├── dbblue-archive-keyring.asc              # your PUBLIC key (safe to publish)
├── setup-dbblue-repo.sh                    # the consumer one-liner (copy it in)
├── pool/<codename>/main/*.deb              # the packages
└── dists/<codename>/
    ├── main/binary-<arch>/Packages(.gz)    # index
    ├── Release  Release.gpg  InRelease      # signed summary
```

Run it once **per release** — on 22.04 it fills `jammy`, on 24.04 `noble`,
etc. A single `dbblue-apt/` serves them all.

> Copy `setup-dbblue-repo.sh` into the repo root so users can curl it:
> `cp packaging/apt/setup-dbblue-repo.sh dbblue-apt/`

## Producer — host it

Anything that serves static files works. Easiest free option — **GitHub Pages**:

1. Create a repo, e.g. `cybrosystech/dbblue-apt`.
2. Push the contents of `./dbblue-apt/` to it.
3. Settings → Pages → serve from the default branch.

It's then live at `https://cybrosystech.github.io/dbblue-apt/` — that URL is
what the consumer scripts point at (override with `DBBLUE_APT_URL`).

## Consumer — install

One-time repo setup, then install:

```bash
curl -fsSL https://cybrosystech.github.io/dbblue-apt/setup-dbblue-repo.sh | sudo sh
sudo apt install dbblue-postgres
```

`apt` pulls in `liblz4-1` and the other dependencies automatically. Updates
later are just `sudo apt update && sudo apt upgrade`.

## Publishing a new version

Build the new `.deb`, re-run `build-repo.sh` with it, and re-upload the
`dbblue-apt/` folder. Users get it with a normal `apt upgrade`.
