#!/usr/bin/env bash
# Nightly TCGCSV catalogue crawl (cron, 12:00 UTC, away from the 02:00 Pokemon update, the
# 03:30 other-games collector and the 21:30 archive backfill).
#
# Runs scripts/download/crawl_tcgcsv_catalogue.py in the running container: set lists and
# product lists for every game, at most 4,000 requests a day, so our TCGCSV total stays well
# under their 10,000-a-day limit. The first full pass takes a few nights; after that each file
# is refreshed monthly. Posts to Discord while the first pass runs, when it completes, and on
# errors; quiet otherwise. It never stops the website or touches DuckDB.
#
# Manual run: scripts/pipeline/run_tcgcsv_catalogue_host.sh (safe to repeat: the daily budget
# and once-a-day rule are kept in data/tcgcsv_catalogue/state.json).
# Output is appended to logs/tcgcsv_catalogue.log.
set -uo pipefail

ROOT="/opt/pokemon-momentum"
LOG_FILE="$ROOT/logs/tcgcsv_catalogue.log"

mkdir -p "$ROOT/logs"
cd "$ROOT"
LOG_OFFSET="$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "[$STARTED_AT] Starting TCGCSV catalogue crawl" >> "$LOG_FILE"
docker-compose exec -T pokemon-momentum python -u scripts/download/crawl_tcgcsv_catalogue.py >> "$LOG_FILE" 2>&1
STATUS=$?
echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Finished (exit $STATUS)" >> "$LOG_FILE"
python3 "$ROOT/scripts/pipeline/notify_discord.py" --job catalogue \
  --exit-code "$STATUS" --log-offset "$LOG_OFFSET" --started-at "$STARTED_AT" >> "$LOG_FILE" 2>&1 || true
exit "$STATUS"
