#!/bin/bash
# packaging/deb/build-deb.sh
# Build the dbblue-postgres .deb from a staged install tree.
#
# Run this AFTER building + staging the tree, e.g. from the repo root:
#     ./configure --prefix=/opt/dbblue/19 --with-lz4 CFLAGS="-O2"
#     make -j"$(nproc)"
#     make install            DESTDIR="$PWD/stage"
#     make -C contrib install DESTDIR="$PWD/stage"
#     packaging/deb/build-deb.sh              # -> dist/dbblue-postgres_<ver>_<arch>.deb
#
# The Depends: line (liblz4-1, libicu*, ...) is GENERATED for whichever OS
# release you build on -- never hand-edit it. Build once per release.
#
#   arg 1     stage dir              (default: ./stage)
#   env VERSION  package version     (default: 19~beta2-1)
#   env OUTDIR   output directory    (default: ./dist)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPL="$HERE/templates"
STAGE="${1:-$PWD/stage}"
OUTDIR="${OUTDIR:-$PWD/dist}"
VERSION="${VERSION:-19~beta2-1}"
PREFIXDIR="opt/dbblue/19"
ARCH="$(dpkg-architecture -qDEB_HOST_ARCH)"

[ -d "$STAGE/$PREFIXDIR/bin" ] || {
  echo "error: no staged tree at $STAGE/$PREFIXDIR"
  echo "       run:  make install DESTDIR=\"$STAGE\"   first"; exit 1; }

# --- guard against the stale-build trap: LZ4 must actually be linked ----------
if ! ldd "$STAGE/$PREFIXDIR/bin/postgres" | grep -qi lz4; then
  echo "error: staged postgres is NOT linked against liblz4."
  echo "       DBblue defaults wal_compression to lz4, so this build would fail at initdb."
  echo "       fix:  make clean && ./configure --with-lz4 ... && make && make install DESTDIR=\"$STAGE\""
  exit 1
fi

mkdir -p "$OUTDIR"

# --- 1. place packaged files into the stage tree -----------------------------
install -d "$STAGE/DEBIAN" "$STAGE/lib/systemd/system" "$STAGE/etc/profile.d" "$STAGE/usr/bin"
install -m 0644 "$TPL/systemd/dbblue.service" "$STAGE/lib/systemd/system/dbblue.service"
install -m 0644 "$TPL/profile.d/dbblue.sh"    "$STAGE/etc/profile.d/dbblue.sh"
install -m 0755 "$TPL/bin/dbblue-status"      "$STAGE/usr/bin/dbblue-status"
install -m 0755 "$TPL/bin/dbblue-psql"        "$STAGE/usr/bin/dbblue-psql"

# --- 2. compute dependencies automatically (scan ALL shipped ELF files) ------
TMP="$(mktemp -d)"; mkdir -p "$TMP/debian"
printf 'Source: dbblue-postgres\nPackage: dbblue-postgres\nArchitecture: any\n' > "$TMP/debian/control"
mapfile -t ELVES < <(find "$STAGE/$PREFIXDIR/bin" "$STAGE/$PREFIXDIR/lib" \
                        -type f \( -perm -u+x -o -name '*.so*' \) 2>/dev/null \
                     | xargs -r file | awk -F: '/ELF/{print $1}')
DEPS="$(cd "$TMP" && dpkg-shlibdeps -O --ignore-missing-info "${ELVES[@]}" 2>/dev/null \
        | sed -n 's/^shlibs:Depends=//p')"
# strip libpq5: we ship our own libpq under /opt/dbblue/19/lib
DEPS="$(echo "$DEPS" | sed -E 's/,[[:space:]]*libpq5[^,]*//g; s/libpq5[^,]*,[[:space:]]*//g')"
[ -n "$DEPS" ] || DEPS="libc6, liblz4-1, zlib1g, libicu70, libreadline8"   # fallback
DEPS="$DEPS, adduser"
rm -rf "$TMP"
echo ">> generated Depends: $DEPS"

# --- 3. control + maintainer scripts -----------------------------------------
sed -e "s|@VERSION@|$VERSION|" -e "s|@ARCH@|$ARCH|" -e "s|@DEPS@|$DEPS|" \
    "$TPL/control" > "$STAGE/DEBIAN/control"
for s in postinst prerm postrm; do install -m 0755 "$TPL/$s" "$STAGE/DEBIAN/$s"; done

# --- 4. build + report -------------------------------------------------------
DEB="$OUTDIR/dbblue-postgres_${VERSION}_${ARCH}.deb"
fakeroot dpkg-deb --build --root-owner-group "$STAGE" "$DEB"
echo ""
echo ">> built: $DEB"
dpkg-deb -f "$DEB" Depends | sed 's/^/   Depends: /'
