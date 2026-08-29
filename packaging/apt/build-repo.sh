#!/bin/bash
# packaging/apt/build-repo.sh
# Assemble / refresh a signed APT repository from one or more .deb files.
#
# Run once per OS release, feeding it that release's .deb(s):
#     GPG_KEY=info@cybrosys.info packaging/apt/build-repo.sh dist/*.deb
#
#   GPG_KEY   email or key id of your signing key (required to sign;
#             without it the repo is built UNSIGNED and apt will reject it)
#   CODENAME  target release pocket (default: this machine's, e.g. jammy/noble)
#   REPO      output repo directory (default: ./dbblue-apt)
#
# Each release gets its own pool/<codename>/ and dists/<codename>/, so a single
# repo dir can serve jammy, noble, bookworm, ... side by side.
set -euo pipefail

REPO="${REPO:-$PWD/dbblue-apt}"
ARCH="$(dpkg-architecture -qDEB_HOST_ARCH)"
GPG_KEY="${GPG_KEY:-}"
CODENAME="${CODENAME:-$( . /etc/os-release 2>/dev/null; echo "${VERSION_CODENAME:-jammy}" )}"

[ "$#" -ge 1 ] || { echo "usage: [GPG_KEY=<email>] [CODENAME=<codename>] $0 <file.deb> [more.deb...]"; exit 1; }

POOL="pool/$CODENAME/main"
DIST="dists/$CODENAME"
mkdir -p "$REPO/$POOL" "$REPO/$DIST/main/binary-$ARCH"
cp -f "$@" "$REPO/$POOL/"

cd "$REPO"

# 1. package index (catalog + checksums) for this release
dpkg-scanpackages --arch "$ARCH" "pool/$CODENAME" /dev/null > "$DIST/main/binary-$ARCH/Packages"
gzip -kf "$DIST/main/binary-$ARCH/Packages"

# 2. release summary
apt-ftparchive \
  -o APT::FTPArchive::Release::Origin="DBblue" \
  -o APT::FTPArchive::Release::Label="DBblue" \
  -o APT::FTPArchive::Release::Suite="$CODENAME" \
  -o APT::FTPArchive::Release::Codename="$CODENAME" \
  -o APT::FTPArchive::Release::Components="main" \
  -o APT::FTPArchive::Release::Architectures="$ARCH" \
  release "$DIST" > "$DIST/Release"

# 3. sign it (apt refuses unsigned repos by default) + export the public key
if [ -n "$GPG_KEY" ]; then
  gpg --default-key "$GPG_KEY" --batch --yes --clearsign -o "$DIST/InRelease"  "$DIST/Release"
  gpg --default-key "$GPG_KEY" --batch --yes -abs        -o "$DIST/Release.gpg" "$DIST/Release"
  gpg --armor --export "$GPG_KEY" > dbblue-archive-keyring.asc
  echo ">> signed with $GPG_KEY"
  echo ">> public key exported -> $REPO/dbblue-archive-keyring.asc"
else
  echo "WARNING: no GPG_KEY given -> repo is UNSIGNED. apt will reject it."
  echo "         create a key, then re-run:  GPG_KEY=<email> $0 <file.deb>"
fi

echo ">> repo ready: $REPO   (codename: $CODENAME, arch: $ARCH)"
