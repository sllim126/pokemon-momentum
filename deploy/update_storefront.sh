#!/usr/bin/env bash
# Update the store on port 8010 (container poke6s-storefront) to the latest `main`.
#
# Why a separate copy: the repo folder /opt/pokemon-momentum is where coding sessions work, and
# they switch branches there. The store used to run from that folder, so it changed (or lost
# checkout) whenever a session checked out an older branch. It now runs from its own clone,
# STOREFRONT_DEPLOY_DIR (default ~/poke6s-store), which only ever holds `main` and changes
# only when this script runs. Data, .env and the compose project stay in the repo folder.
#
# Usage: deploy/update_storefront.sh        (safe to run any time; also does the first set-up)
set -euo pipefail

REPO=/opt/pokemon-momentum
STORE="${STOREFRONT_DEPLOY_DIR:-$HOME/poke6s-store}"

if [ ! -d "$STORE/.git" ]; then
  git clone -q --branch main "$REPO" "$STORE"
fi
if [ -n "$(git -C "$STORE" status --porcelain)" ]; then
  echo "Refusing to update: $STORE has local changes. Make changes in $REPO on a branch instead." >&2
  exit 1
fi
# The clone's origin is the repo folder, so this takes the repo's local `main` (which is what
# gets merged and pushed), whatever branch the folder itself is on.
git -C "$STORE" fetch -q origin main
git -C "$STORE" checkout -q --detach FETCH_HEAD

# The compose file comes from the clone, so its settings match the code. --project-directory
# keeps the same compose project, .env and ./data paths as the repo folder.
export STOREFRONT_CODE_DIR="$STORE/storefront"
docker-compose --project-directory "$REPO" -f "$STORE/docker-compose.yml" up -d --build storefront
# Code is mounted, not baked in: restart so a code-only change is picked up too.
docker-compose --project-directory "$REPO" -f "$STORE/docker-compose.yml" restart storefront

for _ in $(seq 1 20); do
  if health=$(curl -fsS localhost:8010/healthz 2>/dev/null); then
    echo "Store updated to $(git -C "$STORE" log -1 --format='%h %s')"
    echo "Health: $health"
    exit 0
  fi
  sleep 1
done
echo "The store did not answer /healthz within 20 s: docker logs poke6s-storefront" >&2
exit 1
