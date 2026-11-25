#!/usr/bin/env bash
# Nightly store repricing (cron, 03:30 UTC; owner, 2026-11-25).
#
# Runs storefront/tools/reprice.py --apply on the host after the 03:00 market reference job
# (data/storefront/market_reference.json) and the 03:15 store backup. Every price follows its
# rule in data/store_price_rules.csv but never goes under the product's floor: break-even from
# the owner's private costs file, or the rules file's min_price, whichever is higher. Products
# with no cost on file are left alone. Prices changed in the admin since the plan was made are
# skipped, and every change shows in the admin's product history as "repricing".
#
# Safety: --max-market-age-days 2 changes nothing (exit 2, orange in Discord) if the market
# prices weren't refreshed. --max-change-pct 30 holds back any single change over 30% (listed in
# Discord as "held", for the owner to check on the Pricing page). Products priced by hand in the
# admin ("Keep this price") are left alone. The costs file is found through STOREFRONT_COSTS_DIR in .env
# (the same folder the admin's Pricing page reads); only that one line of .env is read, and
# the output holds prices only, never costs.
#
# Manual dry run: python3 storefront/tools/reprice.py --costs "$STOREFRONT_COSTS_DIR/store_sku_costs.csv"
# Output is appended to logs/store_reprice.log; the result goes to Discord like the other jobs.
set -uo pipefail

ROOT="/opt/pokemon-momentum"
LOG_FILE="$ROOT/logs/store_reprice.log"
mkdir -p "$ROOT/logs"
cd "$ROOT"
LOG_OFFSET="$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

COSTS_DIR="$(grep -m1 '^STOREFRONT_COSTS_DIR=' "$ROOT/.env" 2>/dev/null | cut -d= -f2- | tr -d '"'"'"'')"
{
  echo "=== $STARTED_AT store repricing"
  if [[ -z "$COSTS_DIR" || ! -f "$COSTS_DIR/store_sku_costs.csv" ]]; then
    echo "ERROR: the costs file isn't readable (STOREFRONT_COSTS_DIR in .env): nothing changed."
    false
  else
    python3 -u "$ROOT/storefront/tools/reprice.py" --costs "$COSTS_DIR/store_sku_costs.csv" \
      --apply --max-market-age-days 2 --max-change-pct 30 "$@"
  fi
} >> "$LOG_FILE" 2>&1
STATUS=$?
python3 "$ROOT/scripts/pipeline/notify_discord.py" --job store-reprice \
  --exit-code "$STATUS" --log-offset "$LOG_OFFSET" --started-at "$STARTED_AT" >> "$LOG_FILE" 2>&1 || true
exit "$STATUS"
