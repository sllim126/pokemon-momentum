#!/usr/bin/env python3
"""ZFS pool health for Prometheus: scrubs, device errors and true pool capacity.

Runs as root on a ZFS machine (the OpenMediaVault server, 192.168.1.155) every 5 minutes
from zfs-status.timer, and writes zfs_status.prom into node-exporter's text-file folder.
node-exporter's own zfs collector only gives each pool's state (online, degraded...); the
slow problems show up in `zpool status` instead: checksum errors creeping up on one disk,
data errors, or a scrub that hasn't run in months. Standard library only.

Metrics (label zpool on all; device on the device ones):
  zfs_pool_size_bytes, zfs_pool_allocated_bytes, zfs_pool_free_bytes, zfs_pool_fragmentation_percent
  zfs_pool_scan_in_progress{kind}         1 while a scrub or resilver runs
  zfs_pool_scrub_last_end_timestamp_seconds  when the newest finished scrub ended
  zfs_pool_scrub_last_errors               errors that scrub found
  zfs_pool_data_errors                     "errors: N data errors" (files ZFS couldn't repair)
  zfs_pool_device_online{device}           1 when a disk or vdev is ONLINE
  zfs_pool_device_errors{device,kind}      read, write and checksum error counters
  zfs_status_last_run_timestamp_seconds, zfs_status_ok

Install: deploy/monitoring/zfs/install_on_host.sh (runbook "Monitoring", "ZFS on OMV").
`zfs_status.py --stdout` prints instead of writing.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

TEXTFILE_DIR = Path(os.environ.get("ZFS_STATUS_TEXTFILE_DIR", "/var/lib/prometheus/node-exporter"))
OUTPUT = TEXTFILE_DIR / "zfs_status.prom"

HELP = {
    "zfs_pool_size_bytes": "Pool size (zpool list)",
    "zfs_pool_allocated_bytes": "Space allocated in the pool, all datasets and snapshots together",
    "zfs_pool_free_bytes": "Free space in the pool",
    "zfs_pool_fragmentation_percent": "Free-space fragmentation of the pool",
    "zfs_pool_scan_in_progress": "1 while a scrub or resilver (label kind) is running",
    "zfs_pool_scrub_last_end_timestamp_seconds": "When the newest finished scrub ended",
    "zfs_pool_scrub_last_errors": "Errors found by the newest finished scrub",
    "zfs_pool_data_errors": "Data errors ZFS could not repair (zpool status 'errors:' line)",
    "zfs_pool_device_online": "1 when the disk or vdev is ONLINE",
    "zfs_pool_device_errors": "Read, write and checksum error counters per disk or vdev (label kind)",
    "zfs_status_last_run_timestamp_seconds": "When zfs_status.py last ran",
    "zfs_status_ok": "1 when zfs_status.py could read zpool",
}

# "Sun Nov  9 05:36:34 2025" at the end of a scan line (local time).
DATE = r"(\w{3} \w{3} +\d+ \d\d:\d\d:\d\d \d{4})"


def _time(text: str) -> float:
    return time.mktime(time.strptime(re.sub(r" +", " ", text), "%a %b %d %H:%M:%S %Y"))


def parse_status(text: str) -> dict:
    """Parse `zpool status -p <pool>` for one pool. Returns scan, data error and device facts."""
    out: dict = {"devices": [], "in_progress": None, "scrub_end": None, "scrub_errors": None, "data_errors": 0}
    scan = re.search(r"^\s*scan: (.*)$", text, re.M)
    if scan:
        line = scan.group(1)
        if "in progress" in line:
            out["in_progress"] = "resilver" if line.startswith("resilver") else "scrub"
        done = re.match(r"scrub repaired \S+ in \S+ with (\d+) errors on " + DATE, line)
        if done:
            out["scrub_errors"] = int(done.group(1))
            out["scrub_end"] = _time(done.group(2))
    errors = re.search(r"^errors: (\d+) data errors", text, re.M)
    if errors:
        out["data_errors"] = int(errors.group(1))
    # The config table: NAME STATE READ WRITE CKSUM, one row per pool, vdev and disk.
    config = re.search(r"^config:\n(.*?)^errors:", text, re.M | re.S)
    if config:
        rows = [line.split() for line in config.group(1).splitlines() if line.strip()]
        header = next((i for i, r in enumerate(rows) if r[:2] == ["NAME", "STATE"]), None)
        for r in rows[(header + 1) if header is not None else 0:]:
            if len(r) >= 5 and all(x.isdigit() for x in r[2:5]):
                out["devices"].append({"device": r[0], "state": r[1],
                                       "read": int(r[2]), "write": int(r[3]), "checksum": int(r[4])})
    return out


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def collect() -> list[str]:
    lines: list[str] = []
    declared: set[str] = set()

    def add(name: str, value: float | None, **labels: str) -> None:
        if value is None:
            return
        if name not in declared:
            lines.extend([f"# HELP {name} {HELP[name]}", f"# TYPE {name} gauge"])
            declared.add(name)
        text = ",".join(f'{k}="{_escape(str(v))}"' for k, v in labels.items())
        lines.append(f"{name}{{{text}}} {float(value)}" if labels else f"{name} {float(value)}")

    ok = 1
    try:
        listing = subprocess.run(["zpool", "list", "-Hp", "-o", "name,size,allocated,free,fragmentation"],
                                 capture_output=True, text=True, timeout=60, check=True).stdout
        for row in listing.splitlines():
            pool, size, alloc, free, frag = row.split("\t")
            add("zfs_pool_size_bytes", float(size), zpool=pool)
            add("zfs_pool_allocated_bytes", float(alloc), zpool=pool)
            add("zfs_pool_free_bytes", float(free), zpool=pool)
            if frag.rstrip("%").isdigit():
                add("zfs_pool_fragmentation_percent", float(frag.rstrip("%")), zpool=pool)
            status = parse_status(subprocess.run(["zpool", "status", "-p", pool], capture_output=True,
                                                 text=True, timeout=60, check=True).stdout)
            for kind in ("scrub", "resilver"):
                add("zfs_pool_scan_in_progress", 1 if status["in_progress"] == kind else 0, zpool=pool, kind=kind)
            add("zfs_pool_scrub_last_end_timestamp_seconds", status["scrub_end"], zpool=pool)
            add("zfs_pool_scrub_last_errors", status["scrub_errors"], zpool=pool)
            add("zfs_pool_data_errors", status["data_errors"], zpool=pool)
            for dev in status["devices"]:
                if dev["device"] == pool:
                    continue  # the pool's own row; its state is node_zfs_zpool_state
                add("zfs_pool_device_online", 1 if dev["state"] == "ONLINE" else 0, zpool=pool, device=dev["device"])
                for kind in ("read", "write", "checksum"):
                    add("zfs_pool_device_errors", dev[kind], zpool=pool, device=dev["device"], kind=kind)
    except Exception as exc:  # publish the failure instead of a stale file
        print(f"zfs_status: {exc!r}", file=sys.stderr)
        ok = 0
    add("zfs_status_ok", ok)
    add("zfs_status_last_run_timestamp_seconds", time.time())
    return lines


def main(argv: list[str]) -> int:
    text = "\n".join(collect()) + "\n"
    if "--stdout" in argv:
        sys.stdout.write(text)
        return 0
    tmp = OUTPUT.with_name(f".{OUTPUT.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.chmod(0o644)
    tmp.replace(OUTPUT)  # atomic, so node-exporter never reads half a file
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
