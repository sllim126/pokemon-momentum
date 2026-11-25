#!/usr/bin/env bash
# Monthly TCGCSV archive torrent (cron, 01:30 UTC on the 2nd to the 5th of each month).
#
# Runs scripts/torrents/publish_monthly_torrent.py on the host: builds last month's torrent,
# copies it to the PlexVault share, has qBittorrent check and seed it, and emails the owner
# the .torrent with a ready-to-paste #useful-shares post. The last day of a month arrives
# with the archive backfill at ~21:30 UTC on the 1st, so the run on the 2nd normally does it
# all; the runs on the 3rd to the 5th then see the .published marker and stop at once.
# A day still missing means "waiting" (exit 3, grey in Discord, no ping) until the 5th, when
# it becomes a failure. The result goes to Discord, like the other jobs.
#
# Manual run: scripts/pipeline/run_monthly_torrent_host.sh   (or the Python script with --month)
# Output is appended to logs/monthly_torrent.log.
set -uo pipefail

ROOT="/opt/pokemon-momentum"
LOG_FILE="$ROOT/logs/monthly_torrent.log"
FIRST_RUN_DAY="2026-12-02"   # November 2026 is the first monthly torrent (owner, 2026-11-02)
[[ "$(date -u +%F)" < "$FIRST_RUN_DAY" ]] && exit 0

mkdir -p "$ROOT/logs"
cd "$ROOT"
LOG_OFFSET="$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 -u "$ROOT/scripts/torrents/publish_monthly_torrent.py" "$@" >> "$LOG_FILE" 2>&1
STATUS=$?
python3 "$ROOT/scripts/pipeline/notify_discord.py" --job monthly-torrent \
  --exit-code "$STATUS" --log-offset "$LOG_OFFSET" --started-at "$STARTED_AT" >> "$LOG_FILE" 2>&1 || true
exit "$STATUS"
