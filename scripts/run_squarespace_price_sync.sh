#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

# Cron appends this script's output to the sync log; remember where this run starts
# so the Discord summary reads only this run. Manual --dry-run runs don't notify.
SYNC_LOG="${ROOT_DIR}/logs/squarespace_price_sync_cron.log"
LOG_OFFSET="$(stat -c %s "${SYNC_LOG}" 2>/dev/null || echo 0)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

notify() {
  local status=$?
  if [[ "${1:-}" != "--dry-run" ]]; then
    python3 "${ROOT_DIR}/scripts/pipeline/notify_discord.py" \
      --job price-sync \
      --exit-code "${status}" \
      --log-offset "${LOG_OFFSET}" \
      --started-at "${STARTED_AT}" || true
  fi
}
trap 'notify "${DRY_RUN_FLAG:-}"' EXIT

MARKET_CSV="${SQUARESPACE_MARKET_CSV:-}"
if [[ -z "${MARKET_CSV}" ]]; then
  echo "Missing SQUARESPACE_MARKET_CSV in ${ENV_FILE} or environment." >&2
  exit 2
fi

python3 "${ROOT_DIR}/scripts/build_store_price_targets.py"

ARGS=(
  "${ROOT_DIR}/scripts/squarespace_price_sync.py"
  "--market-csv" "${MARKET_CSV}"
)

if [[ -n "${SQUARESPACE_EXPORT_CSV:-}" ]]; then
  ARGS+=("--squarespace-export" "${SQUARESPACE_EXPORT_CSV}")
fi

if [[ -n "${SQUARESPACE_DISCOUNT_PCT:-}" ]]; then
  ARGS+=("--discount-pct" "${SQUARESPACE_DISCOUNT_PCT}")
fi

if [[ -n "${SQUARESPACE_MARKUP_PCT:-}" ]]; then
  ARGS+=("--markup-pct" "${SQUARESPACE_MARKUP_PCT}")
fi

if [[ -n "${SQUARESPACE_MIN_ABS_CHANGE:-}" ]]; then
  ARGS+=("--min-abs-change" "${SQUARESPACE_MIN_ABS_CHANGE}")
fi

if [[ -n "${SQUARESPACE_MIN_PCT_CHANGE:-}" ]]; then
  ARGS+=("--min-pct-change" "${SQUARESPACE_MIN_PCT_CHANGE}")
fi

if [[ "${SQUARESPACE_DISABLE_SALE:-}" =~ ^(1|true|yes)$ ]]; then
  ARGS+=("--disable-sale")
fi

if [[ "${1:-}" == "--dry-run" ]]; then
  ARGS+=("--dry-run")
  DRY_RUN_FLAG="--dry-run"
fi

python3 "${ARGS[@]}"
