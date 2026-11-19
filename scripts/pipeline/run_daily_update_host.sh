#!/usr/bin/env bash
# Nightly market update, run on the host by a systemd timer at 02:00 UTC.
# The Sunday 11:00 UTC weekly refresh runs this same script with
# PIPELINE_ARGS=--full-metadata-refresh (see deploy/systemd/).
#
# The website stays up (since 2026-11-19). DuckDB lets only one process write a file, and
# while the site has prices_db.duckdb open nothing else can write it, so the update works on
# a copy and swaps it in at the end:
#
# 1. copies data/processed/prices_db.duckdb to data/processed_next/ (about 10 GB, a minute
#    or two). The site keeps serving the old file.
# 2. runs scripts/pipeline/run_daily_update.py for each category (English, then Japanese) in
#    one-off containers that see data/processed_next/ as their /app/data/processed,
# 3. runs the store pricing coverage audit (a warning, never a failure),
# 4. runs the extra markets (Lorcana, docs/lorcana_market_plan.md) last, so a failure there
#    can't stop or delay Pokemon; it still marks the run failed,
# 5. checks the copy (opens, has the price table, no unfinished write-ahead log, the live
#    file wasn't changed by another job meanwhile), keeps a hard link to the old file in
#    data/processed_prev/ (one night's rollback) and renames the copy over the live file in
#    one step. The site notices the new file on its next query and reopens it
#    (scripts/dashboards/query_support.py, get_con). Like before, a run that failed partway
#    still swaps in what it finished, as long as the copy checks out.
# 6. posts a pass/fail summary to Discord, even if a step failed.
#
# If the copy can't be made (not enough disk space, or the live file has a write-ahead log),
# it falls back to the old way: stop the site, write the live file, start the site.
#
# Manual run: sudo systemctl start pokemon-momentum-daily-update.service (or run this script).
# Output is appended to logs/daily_update.log. Optional env: DAILY_WORKERS,
# PIPELINE_ARGS (extra flags for every category), DAILY_CATEGORY_IDS, EXTRA_CATEGORY_IDS
# (default "71"; set it to "" to switch Lorcana off), NOTIFY_TITLE, SWAP (default 1; 0 =
# the old stop-the-site way), NOTIFY (default 1; 0 = no Discord summary, for manual tests),
# LOCK_DIR, LOG_DIR (manual tests as a normal user). Rollback after a bad night: see docs/operator_runbook.txt, "Nightly update".
set -euo pipefail

ROOT="/opt/pokemon-momentum"
LOG_DIR="${LOG_DIR:-$ROOT/logs}"
LOCK_DIR="${LOCK_DIR:-$ROOT/.locks}"
LOCK_FILE="$LOCK_DIR/daily_update.lock"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SERVICE_NAME="pokemon-momentum"
# Keep scheduled runs modest so the nightly job stays stable under normal load.
DAILY_WORKERS="${DAILY_WORKERS:-4}"
PIPELINE_ARGS="${PIPELINE_ARGS:-}"
# ${VAR-default} (no colon): DAILY_CATEGORY_IDS="" really means none (manual tests of the
# copy and swap without re-running Pokemon, whose group step asks TCGCSV again).
DAILY_CATEGORY_IDS="${DAILY_CATEGORY_IDS-3 85}"
# ${VAR-default} (no colon): an empty EXTRA_CATEGORY_IDS="" really means none.
EXTRA_CATEGORY_IDS="${EXTRA_CATEGORY_IDS-71}"
SWAP="${SWAP:-1}"
NOTIFY="${NOTIFY:-1}"
LIVE_DB="$ROOT/data/processed/prices_db.duckdb"
NEXT_DIR="$ROOT/data/processed_next"
NEXT_DB="$NEXT_DIR/prices_db.duckdb"
PREV_DIR="$ROOT/data/processed_prev"
# Room to leave on the disk beyond the copy itself (bytes).
SPARE_BYTES=$((5 * 1024 * 1024 * 1024))
# Set while running: extra `docker-compose run` flags (the copy's mount), whether a copy is
# waiting to be swapped in, the live file's identity when copied, whether the site was stopped.
MOUNT_ARGS=()
SWAP_PENDING=0
LIVE_ID=""
SITE_STOPPED=0

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
  [[ "$NOTIFY" == "1" ]] || return 0
  python3 "$ROOT/scripts/pipeline/notify_discord.py" \
    --exit-code "$1" \
    --log-offset "$LOG_OFFSET" \
    --started-at "$STAMP" \
    --title "$NOTIFY_TITLE" >> "$LOG_DIR/daily_update.log" 2>&1 || true
}

echo "[$STAMP] Starting Pokemon Momentum daily update" | tee -a "$LOG_DIR/daily_update.log"

# Expected result: name lookups work before anything is stopped. After a power cut the
# timer's catch-up run starts at boot, sometimes before DNS is ready (2026-10-15 it failed
# with "Temporary failure in name resolution"). Wait up to 10 minutes; this is a local DNS
# check and sends nothing to TCGCSV.
for _ in $(seq 1 60); do
  getent hosts tcgcsv.com >/dev/null 2>&1 && break
  sleep 10
done
if ! getent hosts tcgcsv.com >/dev/null 2>&1; then
  echo "[$STAMP] ERROR: tcgcsv.com still can't be resolved after 10 minutes; network not ready" | tee -a "$LOG_DIR/daily_update.log"
  notify 1
  exit 1
fi

if ! docker-compose ps "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "[$STAMP] ERROR: docker-compose service lookup failed" | tee -a "$LOG_DIR/daily_update.log"
  notify 1
  exit 1
fi

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG_DIR/daily_update.log"
}

stop_site() {
  # The old way, used only when no copy can be made: release the DuckDB lock by stopping.
  log "Stopping $SERVICE_NAME to release the DuckDB lock (no copy this run)"
  docker-compose stop "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1
  SITE_STOPPED=1
}

prepare_copy() {
  # Expected result: data/processed_next/prices_db.duckdb is a copy of the live database and
  # every pipeline container below writes to it, or (no room, or an unfinished write-ahead
  # log on the live file) the site is stopped and the live file is written as before.
  if [[ "$SWAP" != "1" ]]; then
    stop_site
    return
  fi
  if [[ -e "$LIVE_DB.wal" ]]; then
    log "Warning: $LIVE_DB.wal exists (an unfinished write); updating in place this time"
    stop_site
    return
  fi
  local size avail
  size="$(stat -c %s "$LIVE_DB")"
  avail="$(df --output=avail -B1 "$ROOT/data" | tail -1 | tr -d ' ')"
  if (( avail < size + SPARE_BYTES )); then
    log "Warning: not enough disk space for a copy ($((avail / 1073741824)) GB free); updating in place this time"
    stop_site
    return
  fi
  rm -rf "$NEXT_DIR"
  mkdir -p "$NEXT_DIR"
  LIVE_ID="$(stat -c '%d-%i-%Y-%s' "$LIVE_DB")"
  log "Copying the price database ($((size / 1073741824)) GB) so the site stays up"
  if ! cp --sparse=always --preserve=mode,ownership,timestamps "$LIVE_DB" "$NEXT_DB"; then
    log "Warning: copying the database failed; updating in place this time"
    rm -rf "$NEXT_DIR"
    stop_site
    return
  fi
  MOUNT_ARGS=(-v "$NEXT_DIR:/app/data/processed")
  SWAP_PENDING=1
  log "Copy ready; the pipeline writes to it while the site serves the old file"
}

swap_in() {
  # Expected result: the updated copy replaces the live file in one rename, or the live file
  # is left alone and the reason is logged (returns 1).
  SWAP_PENDING=0
  if [[ -e "$NEXT_DB.wal" ]]; then
    log "ERROR: the copy has an unfinished write-ahead log; not swapping it in (site keeps last night's data)"
    return 1
  fi
  if [[ "$(stat -c '%d-%i-%Y-%s' "$LIVE_DB")" != "$LIVE_ID" ]]; then
    log "ERROR: the live database changed during the run (another job wrote it); not swapping in tonight's copy"
    return 1
  fi
  if ! docker-compose run --rm -T "${MOUNT_ARGS[@]}" "$SERVICE_NAME" python -c "
import duckdb
con = duckdb.connect('/app/data/processed/prices_db.duckdb', read_only=True)
tables = {row[0] for row in con.execute('SHOW TABLES').fetchall()}
table = 'pokemon_prices' if 'pokemon_prices' in tables else 'prices'
rows, latest = con.execute(f'SELECT COUNT(*), MAX(date) FROM {table}').fetchone()
assert rows > 0, 'no price rows'
print(f'Copy checks out: {rows} price rows, latest {latest}')
" 2>&1 | tee -a "$LOG_DIR/daily_update.log"; then
    log "ERROR: the copy failed its check; not swapping it in (site keeps last night's data)"
    return 1
  fi
  local extra
  # Expected extras, not swapped: .open (query_support's per-file links) and empty
  # tracking/visitors databases created when a step imports the web server's modules.
  extra="$(find "$NEXT_DIR" -mindepth 1 -maxdepth 1 ! -name prices_db.duckdb ! -name .open \
    ! -name tracking.sqlite3 ! -name visitors.sqlite3 -printf '%f ' 2>/dev/null)"
  [[ -z "$extra" ]] || log "Warning: the pipeline also wrote $extra into its processed folder; those are not swapped in"
  chown --reference="$LIVE_DB" "$NEXT_DB" && chmod --reference="$LIVE_DB" "$NEXT_DB" || true
  mkdir -p "$PREV_DIR"
  # Yesterday's file stays reachable for a rollback; linking over the older one frees it.
  ln -f "$LIVE_DB" "$PREV_DIR/prices_db.duckdb"
  # rename(2): the live name points at the new file in one step; the site's open file is
  # untouched until it reopens.
  mv -fT "$NEXT_DB" "$LIVE_DB"
  rm -rf "$NEXT_DIR"
  log "Swapped in the new price database; the site stayed up"
  # Wake the site so it reopens the new file now rather than on the next visitor.
  curl -s -o /dev/null --max-time 120 "http://127.0.0.1:8001/health" || true
}

cleanup() {
  local status=$?
  if [[ "$SWAP_PENDING" == "1" ]]; then
    swap_in || status=1
  fi
  rm -rf "$NEXT_DIR"
  if [[ "$SITE_STOPPED" == "1" ]]; then
    # Expected result: the app service comes back even if the pipeline fails mid-run.
    log "Restarting $SERVICE_NAME service"
    docker-compose up -d "$SERVICE_NAME" >> "$LOG_DIR/daily_update.log" 2>&1 || true
  fi
  notify "$status"
  exit "$status"
}

trap cleanup EXIT

prepare_copy

# Expected result: one-off container runs the pipeline and appends a full audit trail to the log.
for CATEGORY_ID in $DAILY_CATEGORY_IDS; do
  echo "[$STAMP] Running pipeline for category ${CATEGORY_ID}"
  docker-compose run --rm -T "${MOUNT_ARGS[@]}" "$SERVICE_NAME" bash -lc "
    set -euo pipefail
    cd /app
    python scripts/pipeline/run_daily_update.py --category-id ${CATEGORY_ID} --workers ${DAILY_WORKERS} ${PIPELINE_ARGS}
  " 2>&1 | tee -a "$LOG_DIR/daily_update.log"
done

echo "[$STAMP] Running store pricing coverage audit"
if ! docker-compose run --rm -T "${MOUNT_ARGS[@]}" "$SERVICE_NAME" bash -lc "
  set -euo pipefail
  cd /app
  python scripts/utilities/audit_store_pricing_coverage.py
" 2>&1 | tee -a "$LOG_DIR/daily_update.log"; then
  AUDIT_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "[$AUDIT_STAMP] Warning: store pricing coverage audit reported missing_unexpected SKUs" | tee -a "$LOG_DIR/daily_update.log"
fi

# Expected result: extra markets update after Pokemon is safely done. A failure is logged and
# remembered, the other extra markets still run, and the job exits 1 at the end.
EXTRA_FAILED=0
for CATEGORY_ID in $EXTRA_CATEGORY_IDS; do
  echo "[$STAMP] Running pipeline for category ${CATEGORY_ID}"
  if ! docker-compose run --rm -T "${MOUNT_ARGS[@]}" "$SERVICE_NAME" bash -lc "
    set -euo pipefail
    cd /app
    python scripts/pipeline/run_daily_update.py --category-id ${CATEGORY_ID} --workers ${DAILY_WORKERS} ${PIPELINE_ARGS}
  " 2>&1 | tee -a "$LOG_DIR/daily_update.log"; then
    echo "[$STAMP] ERROR: pipeline for category ${CATEGORY_ID} failed" | tee -a "$LOG_DIR/daily_update.log"
    EXTRA_FAILED=1
  fi
done

END_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "[$END_STAMP] Daily update finished"
# The EXIT trap swaps in the copy (or restarts the site) and reports this status to Discord.
exit "$EXTRA_FAILED"
