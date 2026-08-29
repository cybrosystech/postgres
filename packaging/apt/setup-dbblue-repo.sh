#!/bin/sh
# DBblue APT repository setup — the consumer's one-time step.
# (This is DBblue's equivalent of PostgreSQL's apt.postgresql.org.sh.)
#
#   curl -fsSL https://cybrosystech.github.io/dbblue-apt/setup-dbblue-repo.sh | sudo sh
# or download it and:
#   sudo sh setup-dbblue-repo.sh
set -e

BASE="${DBBLUE_APT_URL:-https://cybrosystech.github.io/dbblue-apt}"
CODENAME="$( . /etc/os-release; echo "$VERSION_CODENAME" )"

if [ "$(id -u)" -ne 0 ]; then
  echo "Please run as root (use sudo)."; exit 1
fi

echo "Adding DBblue repository for '$CODENAME' from $BASE ..."
curl -fsSL "$BASE/dbblue-archive-keyring.asc" | gpg --dearmor -o /usr/share/keyrings/dbblue.gpg
echo "deb [signed-by=/usr/share/keyrings/dbblue.gpg] $BASE $CODENAME main" \
  > /etc/apt/sources.list.d/dbblue.list
apt-get update

echo ""
echo "DBblue repository added. Install with:"
echo "    sudo apt install dbblue-postgres"
