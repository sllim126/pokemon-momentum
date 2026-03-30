#!/usr/bin/env bash
set -euo pipefail

ROOT="/opt/pokemon-momentum"
LOG_DIR="$ROOT/logs"
LOCK_DIR="$ROOT/.locks"
LOCK_FILE="$LOCK_DIR/daily_update.lock"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SERVICE_NAME="pokemon-momentum"
# Keep scheduled runs modest so the nightly job stays stable under normal load.
DAILY_WORKERS="${DAILY_WORKERS:-4}"
PIPELINE_ARGS="${PIPELINE_ARGS:-}"
DAILY_CATEGORY_IDS="${DAILY_CATEGORY_IDS:-3 85}"

mkdir -p "$LOG_DIR" "$LOCK_DIR"

exec 9>"$LOCK_FILE"
# Expected result: only one host-driven update can run at a time.
if ! flock -n 9; then
  echo "[$STAMP] Daily update already running; exiting."
  exit 0
fi

cd "$ROOT"

echo "[$STAMP] Starting Pokemon Momentum daily update"

if ! docker-compose ps "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "[$STAMP] docker-compose service lookup failed"
  exit 1
fi

cleanup() {
  local end_stamp
  end_stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Expected result: the app service comes back even if the pipeline fails mid-run.
  echo "[$end_stamp] Restarting $SERVICE_NAME service"
  docker-compose up -d "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1 || true
}

trap cleanup EXIT

# Expected result: release the DuckDB file lock before running the write-heavy pipeline.
echo "[$STAMP] Stopping $SERVICE_NAME to release DuckDB lock"
docker-compose stop "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1

# Expected result: one-off container runs the pipeline and appends a full audit trail to the log.
for CATEGORY_ID in $DAILY_CATEGORY_IDS; do
  echo "[$STAMP] Running pipeline for category ${CATEGORY_ID}"
  docker-compose run --rm -T "$SERVICE_NAME" bash -lc "
    set -euo pipefail
    cd /app
    python scripts/pipeline/run_daily_update.py --category-id ${CATEGORY_ID} --workers ${DAILY_WORKERS} ${PIPELINE_ARGS}
  " 2>&1 | tee -a "$LOG_DIR/daily_update.log"
done

END_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "[$END_STAMP] Daily update finished"
