#!/usr/bin/env bash
# Turn on SMART disk health for Prometheus on a Debian machine (OpenMediaVault, Proxmox, PBS),
# from this server. Usage: deploy/monitoring/smart/install_on_host.sh root@192.168.1.155
#
# Installs smartmontools and Debian's prometheus-node-exporter-collectors, whose smartmon timer
# runs smartctl on every disk every 15 minutes and writes smartmon.prom into node-exporter's
# text-file folder. The Backup Server (192.168.1.72) already had this. Safe to rerun.
# The machine must already run node-exporter (the Debian package, text-file folder
# /var/lib/prometheus/node-exporter) and be listed in deploy/monitoring/nodes.yml.
# The disks show in "Disk health (SMART)" on Home lab: Servers within 15 minutes.
# Removing it: runbook "Monitoring", "SMART".
set -euo pipefail
HOST="${1:?usage: $0 root@<host>}"
ssh "$HOST" 'set -e
  test -d /var/lib/prometheus/node-exporter || { echo "no /var/lib/prometheus/node-exporter: is node-exporter installed?"; exit 1; }
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  # moreutils: the timer writes through sponge, so a half-written file is never scraped.
  apt-get install -y --no-install-recommends smartmontools moreutils prometheus-node-exporter-collectors
  systemctl enable --now prometheus-node-exporter-smartmon.timer
  systemctl start prometheus-node-exporter-smartmon.service
  echo "disks seen by smartctl:"
  grep "^smartmon_device_info" /var/lib/prometheus/node-exporter/smartmon.prom | sed "s/.*disk=\"\([^\"]*\)\".*/  \1/" | sort -u'
echo "installed on $HOST"
