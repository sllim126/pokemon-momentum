#!/usr/bin/env bash
# Nightly market update, run on the host by a systemd timer at 02:00 UTC.
# The Sunday 11:00 UTC weekly refresh runs this same script with
# PIPELINE_ARGS=--full-metadata-refresh (see deploy/systemd/).
#
# 1. stops the website container (DuckDB allows only one writer),
# 2. runs scripts/pipeline/run_daily_update.py for each category (English, then Japanese),
# 3. runs the store pricing coverage audit (a warning, never a failure),
# 4. restarts the website and posts a pass/fail summary to Discord, even if a step failed.
#
# Manual run: sudo systemctl start pokemon-momentum-daily-update.service (or run this script).
# Output is appended to logs/daily_update.log. Optional env: DAILY_WORKERS,
# PIPELINE_ARGS (extra flags for every category), DAILY_CATEGORY_IDS, NOTIFY_TITLE.
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

# Remember where this run's log output starts so the Discord summary only reads this run.
LOG_OFFSET="$(stat -c %s "$LOG_DIR/daily_update.log" 2>/dev/null || echo 0)"
NOTIFY_TITLE="${NOTIFY_TITLE:-Nightly market update}"

notify() {
  # Expected result: a pass/fail summary lands in Discord; never fails the job itself.
  python3 "$ROOT/scripts/pipeline/notify_discord.py" \
    --exit-code "$1" \
    --log-offset "$LOG_OFFSET" \
    --started-at "$STAMP" \
    --title "$NOTIFY_TITLE" >> "$LOG_DIR/daily_update.log" 2>&1 || true
}

echo "[$STAMP] Starting Pokemon Momentum daily update" | tee -a "$LOG_DIR/daily_update.log"

if ! docker-compose ps "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "[$STAMP] ERROR: docker-compose service lookup failed" | tee -a "$LOG_DIR/daily_update.log"
  notify 1
  exit 1
fi

cleanup() {
  local status=$?
  local end_stamp
  end_stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # Expected result: the app service comes back even if the pipeline fails mid-run.
  echo "[$end_stamp] Restarting $SERVICE_NAME service"
  docker-compose up -d "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1 || true
  notify "$status"
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

echo "[$STAMP] Running store pricing coverage audit"
if ! docker-compose run --rm -T "$SERVICE_NAME" bash -lc "
  set -euo pipefail
  cd /app
  python scripts/utilities/audit_store_pricing_coverage.py
" 2>&1 | tee -a "$LOG_DIR/daily_update.log"; then
  AUDIT_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "[$AUDIT_STAMP] Warning: store pricing coverage audit reported missing_unexpected SKUs" | tee -a "$LOG_DIR/daily_update.log"
fi

END_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "[$END_STAMP] Daily update finished"
