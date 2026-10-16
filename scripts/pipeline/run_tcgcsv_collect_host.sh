#!/usr/bin/env bash
# Nightly collect-only archive of other games' TCGCSV prices (systemd timer
# pokemon-momentum-tcgcsv-collect.timer, 03:30 UTC, as user adam; catches up after downtime).
#
# Runs scripts/download/collect_tcgcsv_categories.py in a one-off container. It saves
# Magic, Yu-Gi-Oh, One Piece and Lorcana prices to data/tcgcsv_archive/ and never touches
# DuckDB, so the website keeps running. It waits for the Pokemon nightly update's lock
# first, so the two jobs never fetch from TCGCSV at the same time, then posts a summary
# to Discord.
#
# Manual run: scripts/pipeline/run_tcgcsv_collect_host.sh
# Output is appended to logs/tcgcsv_collect.log.
set -euo pipefail

ROOT="/opt/pokemon-momentum"
LOG_FILE="$ROOT/logs/tcgcsv_collect.log"
LOCK_FILE="$ROOT/.locks/daily_update.lock"
SERVICE_NAME="pokemon-momentum"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p "$ROOT/logs"
cd "$ROOT"
LOG_OFFSET="$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)"

notify() {
  # Expected result: a pass/fail summary lands in Discord; never fails the job itself.
  local status=$?
  python3 "$ROOT/scripts/pipeline/notify_discord.py" --job collect \
    --exit-code "$status" --log-offset "$LOG_OFFSET" --started-at "$STARTED_AT" >> "$LOG_FILE" 2>&1 || true
}
trap notify EXIT

{
  echo "[$STARTED_AT] Starting TCGCSV collect-only archive"
  # Wait up to 10 minutes for DNS after a reboot (local check, nothing sent to TCGCSV).
  for _ in $(seq 1 60); do getent hosts tcgcsv.com >/dev/null 2>&1 && break; sleep 10; done
  # Wait up to 4 hours for the Pokemon nightly update to finish (it holds this lock), then
  # let go of it straight away: holding it would make a Pokemon run that starts meanwhile
  # skip its day. The file is opened read-only because it belongs to root (the nightly
  # update runs as root); flock works on a read-only descriptor.
  if [[ -e "$LOCK_FILE" ]] && ! flock -w 14400 "$LOCK_FILE" true; then
    echo "ERROR: Pokemon nightly update still running after 4 hours; skipping today's collection."
    exit 1
  fi
  docker-compose run --rm -T "$SERVICE_NAME" python -u scripts/download/collect_tcgcsv_categories.py
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] TCGCSV collect-only archive finished"
} >> "$LOG_FILE" 2>&1
