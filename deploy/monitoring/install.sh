#!/usr/bin/env bash
# Install or update the Grafana monitoring for Pokemon Momentum on this host.
# Safe to run again after changing anything in deploy/monitoring/. Needs sudo.
# Full description: "Monitoring" in docs/operator_runbook.txt.
#
# What it does:
#   1. node-exporter's text-file folder: group adam, group-writable, so the cron jobs
#      (run as adam) can write their status files there.
#   2. blackbox exporter (apt package prometheus-blackbox-exporter): listen on localhost only.
#   3. Prometheus: copy prometheus_momentum.yml to /etc/prometheus/momentum_scrape.yml and
#      make prometheus.yml load it (a dated backup of prometheus.yml is kept first).
#   4. Grafana: install the data source and dashboard provisioning files.
#   5. cAdvisor container (docker-compose.yml in this folder).
# The crontab lines (collector every minute, run_job.py around each cron job) are separate:
# see deploy/crontab.txt.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TEXTFILE_DIR=/var/lib/prometheus/node-exporter
PROM_MAIN=/etc/prometheus/prometheus.yml
PROM_EXTRA=/etc/prometheus/momentum_scrape.yml

echo "== text-file folder"
sudo chgrp adam "$TEXTFILE_DIR"
sudo chmod 2775 "$TEXTFILE_DIR"

echo "== blackbox exporter (localhost only)"
if ! grep -q 'web.listen-address' /etc/default/prometheus-blackbox-exporter; then
  sudo sed -i 's|^ARGS="\(.*\)"|ARGS="\1 --web.listen-address=127.0.0.1:9115"|' /etc/default/prometheus-blackbox-exporter
  sudo systemctl restart prometheus-blackbox-exporter
fi

echo "== Prometheus"
sudo install -m 644 "$HERE/prometheus_momentum.yml" "$PROM_EXTRA"
if ! grep -q "^scrape_config_files:" "$PROM_MAIN"; then
  sudo cp -p "$PROM_MAIN" "$PROM_MAIN.bak-$(date +%Y%m%d%H%M%S)"
  printf '\n# Pokemon Momentum scrape jobs (deploy/monitoring/install.sh)\nscrape_config_files:\n  - %s\n' "$PROM_EXTRA" \
    | sudo tee -a "$PROM_MAIN" >/dev/null
fi
promtool check config "$PROM_MAIN"
sudo systemctl reload prometheus || sudo systemctl restart prometheus

echo "== Grafana"
sudo install -m 644 -g grafana "$HERE/grafana/provisioning/datasources.yml" \
  /etc/grafana/provisioning/datasources/pokemon-momentum.yml
sudo install -m 644 -g grafana "$HERE/grafana/provisioning/dashboards.yml" \
  /etc/grafana/provisioning/dashboards/pokemon-momentum.yml
# Open Grafana on the Overview dashboard (grafana-server reads this file as its environment).
HOME_LINE="GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=$HERE/grafana/dashboards/overview.json"
if ! sudo grep -qxF "$HOME_LINE" /etc/default/grafana-server; then
  sudo sed -i '/^GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/d' /etc/default/grafana-server
  echo "$HOME_LINE" | sudo tee -a /etc/default/grafana-server >/dev/null
fi
sudo systemctl restart grafana-server

echo "== cAdvisor"
docker compose -f "$HERE/docker-compose.yml" up -d

echo "Done. Grafana: http://192.168.1.240:3000 (folder 'Pokemon Momentum')."
