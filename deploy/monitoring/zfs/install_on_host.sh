#!/usr/bin/env bash
# Install or update the ZFS pool health collector on a ZFS machine, from this server.
# Usage: deploy/monitoring/zfs/install_on_host.sh root@192.168.1.155
# Copies zfs_status.py and its systemd timer, runs it once and shows the result. Safe to rerun.
# The machine must already run node-exporter with the text-file folder
# /var/lib/prometheus/node-exporter (the Debian package's default). Removing it: runbook
# "Monitoring", "ZFS on OMV".
set -euo pipefail
HOST="${1:?usage: $0 root@<host>}"
HERE="$(cd "$(dirname "$0")" && pwd)"
scp "$HERE/zfs_status.py" "$HERE/zfs-status.service" "$HERE/zfs-status.timer" "$HOST:/tmp/"
ssh "$HOST" 'set -e
  install -d /usr/local/lib/zfs-status
  install -m 755 /tmp/zfs_status.py /usr/local/lib/zfs-status/zfs_status.py
  install -m 644 /tmp/zfs-status.service /tmp/zfs-status.timer /etc/systemd/system/
  rm -f /tmp/zfs_status.py /tmp/zfs-status.service /tmp/zfs-status.timer
  test -d /var/lib/prometheus/node-exporter || { echo "no /var/lib/prometheus/node-exporter: is node-exporter installed?"; exit 1; }
  systemctl daemon-reload
  systemctl enable --now zfs-status.timer
  systemctl start zfs-status.service
  grep -v "^#" /var/lib/prometheus/node-exporter/zfs_status.prom'
echo "installed on $HOST"
