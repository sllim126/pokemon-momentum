#!/usr/bin/env bash
set -euo pipefail

ROOT="/opt/pokemon-momentum"
LOG_DIR="$ROOT/logs"
LOCK_DIR="$ROOT/.locks"
LOCK_FILE="$LOCK_DIR/metadata_placeholder_refresh.lock"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SERVICE_NAME="pokemon-momentum"
CATEGORY_ID="${CATEGORY_ID:-3}"

mkdir -p "$LOG_DIR" "$LOCK_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$STAMP] Metadata placeholder refresh already running; exiting."
  exit 0
fi

cd "$ROOT"

echo "[$STAMP] Starting Pokemon Momentum metadata placeholder refresh"

if ! docker-compose ps "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "[$STAMP] docker-compose service lookup failed"
  exit 1
fi

cleanup() {
  local end_stamp
  end_stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "[$end_stamp] Restarting $SERVICE_NAME service"
  docker-compose up -d "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1 || true
}

trap cleanup EXIT

echo "[$STAMP] Stopping $SERVICE_NAME to release DuckDB lock"
docker-compose stop "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1

docker-compose run --rm -T "$SERVICE_NAME" bash -lc "
  set -euo pipefail
  cd /app
  python scripts/utilities/export_products_for_my_groups.py --category-id ${CATEGORY_ID}
  python scripts/indicators/build_product_signal_snapshot.py --category-id ${CATEGORY_ID}
" 2>&1 | tee -a "$LOG_DIR/daily_update.log"

END_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "[$END_STAMP] Metadata placeholder refresh finished"
