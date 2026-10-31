#!/usr/bin/env bash
# Nightly TCGCSV archive backfill (cron, 21:30 UTC, after TCGCSV's ~20:00 UTC refresh).
#
# Runs scripts/download/backfill_tcgcsv_archive.py in the running container. While TCGCSV's
# archive is offline that is one request a night (a 403, posted to Discord in grey). The
# night it returns, it downloads every missing day (gap since 2026-09-16) and posts the list;
# after that it fetches the one new day each night and posts only when something goes wrong.
# The owner asked for the first run on 2026-11-01 (TCGCSV's planned return), so earlier runs
# exit quietly.
#
# Manual run: scripts/pipeline/run_archive_backfill_host.sh
# Output is appended to logs/archive_backfill.log.
set -uo pipefail

ROOT="/opt/pokemon-momentum"
LOG_FILE="$ROOT/logs/archive_backfill.log"
FIRST_RUN_DAY="2026-11-01"
[[ "$(date -u +%F)" < "$FIRST_RUN_DAY" ]] && exit 0

mkdir -p "$ROOT/logs"
cd "$ROOT"
LOG_OFFSET="$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "[$STARTED_AT] Starting TCGCSV archive backfill" >> "$LOG_FILE"
docker-compose exec -T pokemon-momentum python -u scripts/download/backfill_tcgcsv_archive.py >> "$LOG_FILE" 2>&1
STATUS=$?
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finished (exit $STATUS)" >> "$LOG_FILE"
python3 "$ROOT/scripts/pipeline/notify_discord.py" --job archive-backfill \
  --exit-code "$STATUS" --log-offset "$LOG_OFFSET" --started-at "$STARTED_AT" >> "$LOG_FILE" 2>&1 || true
exit "$STATUS"
