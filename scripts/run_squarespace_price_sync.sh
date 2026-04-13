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
fi

exec python3 "${ARGS[@]}"
