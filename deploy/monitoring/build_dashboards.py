#!/usr/bin/env python3
"""Build the Grafana dashboards for Pokemon Momentum as JSON files.

    python3 deploy/monitoring/build_dashboards.py

writes grafana/dashboards/*.json next to this file. Grafana loads that folder by itself
(provisioning in grafana/provisioning/dashboards.yml) within 30 s; no restart needed.
Change the dashboards here, not in the Grafana UI: UI edits are lost when the file is
rebuilt. The JSON is committed too, so the dashboards work without running this script.

Five dashboards, all reading the Prometheus data source (uid "prometheus"):

* Overview: is everything all right? One screen of status tiles, the jobs table and the
  headline charts. Grafana's home page.
* Website: requests, errors and response times per app and route, and outside probes.
* Servers: this host (CPU, memory, disks, network, updates) and every Docker container.
* Jobs & work: scheduled jobs, data downloads, logs, docs/todo.txt, bug reports, git.
* Home lab: the Proxmox cluster (nodes, VMs, containers, storage) and the UniFi network.

Where the numbers come from: docs/operator_runbook.txt, "Monitoring".

Design rules (from the team's chart guidance): stat tiles for headline numbers; green /
orange / red only for status, always with the value or a word beside it; thin lines; one
y-axis per chart.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}

# --- shared PromQL -------------------------------------------------------------------------

# A job is overdue when its last success is older than 1.5 x its interval plus an hour of
# grace (the nightly update alone takes ~20 minutes). Intervals come from collect_status.py.
LAST_SUCCESS = "max by (task) (momentum_job_last_success_timestamp_seconds)"
INTERVAL = "max by (task) (momentum_job_expected_interval_seconds)"
OVERDUE_LIMIT = f"(1.5 * {INTERVAL} + 3600)"
OVERDUE_BOOL = f"((time() - {LAST_SUCCESS}) > bool {OVERDUE_LIMIT})"
FAILED_BOOL = "(max by (task) (momentum_job_last_exit_code) != bool 0)"
# 0 ok, 1 overdue, 2 failed, 3 failed and overdue, 4 no run recorded yet.
JOB_STATUS = f"(2 * {FAILED_BOOL} + {OVERDUE_BOOL}) or ({INTERVAL} * 0 + 4)"
JOBS_FAILED = f"count({FAILED_BOOL} == 1) or vector(0)"
JOBS_OVERDUE = f"count((time() - {LAST_SUCCESS}) > {OVERDUE_LIMIT}) or vector(0)"

PRICE_AGE = "time() - market_latest_data_timestamp_seconds"
ROOT_DISK_USED = (
    '100 * (1 - node_filesystem_avail_bytes{mountpoint="/",fstype!="rootfs"}'
    ' / node_filesystem_size_bytes{mountpoint="/",fstype!="rootfs"})'
)
MEM_USED = "100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)"
CPU_USED = '100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))'

DAY = 86400

# --- panel helpers -------------------------------------------------------------------------


def steps(*pairs):
    """steps("green", (80, "orange"), (90, "red")) -> Grafana threshold steps."""
    out = [{"color": pairs[0], "value": None}]
    out += [{"color": color, "value": value} for value, color in pairs[1:]]
    return {"mode": "absolute", "steps": out}


def value_map(mapping: dict) -> list:
    """{1: ("UP", "green"), ...} -> a Grafana value mapping."""
    return [{
        "type": "value",
        "options": {str(k): {"text": text, "color": color, "index": i}
                    for i, (k, (text, color)) in enumerate(mapping.items())},
    }]


UP_DOWN = value_map({1: ("UP", "green"), 0: ("DOWN", "red")})
JOB_STATUS_MAP = value_map({
    0: ("OK", "green"), 1: ("Overdue", "orange"), 2: ("Failed", "red"),
    3: ("Failed, overdue", "red"), 4: ("No run recorded yet", "text"),
})


def target(expr, legend="", ref="A", instant=False, table=False):
    t = {"refId": ref, "expr": expr, "legendFormat": legend, "datasource": DS}
    if instant:
        t["instant"], t["range"] = True, False
    if table:
        t["format"] = "table"
    return t


def stat(title, expr, pos, unit="none", thresholds=None, mappings=None, decimals=None,
         description="", link=None):
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": thresholds or steps("green"), "mappings": mappings or []}
    if decimals is not None:
        defaults["decimals"] = decimals
    if link:
        defaults["links"] = [{"title": link[0], "url": link[1]}]
    return {
        "type": "stat", "title": title, "description": description, "gridPos": pos, "datasource": DS,
        "targets": [target(expr, instant=True)],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "background", "graphMode": "none", "textMode": "value",
                    "justifyMode": "center", "wideLayout": True},
    }


def timeseries(title, targets, pos, unit="none", description="", stack=False, min_zero=True,
               thresholds=None):
    custom = {"lineWidth": 2, "fillOpacity": 10 if stack else 0, "showPoints": "never",
              "spanNulls": True, "axisSoftMin": 0 if min_zero else None,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if thresholds:
        defaults["thresholds"] = thresholds
        custom["thresholdsStyle"] = {"mode": "dashed"}
    return {
        "type": "timeseries", "title": title, "description": description, "gridPos": pos,
        "datasource": DS, "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def table(title, targets, pos, columns, description="", overrides=None, sort=None, cell_color=True):
    """Instant queries joined on their labels. columns maps the raw field names Grafana
    produces ("task", "Value #A", ...) to headings, in display order; others are hidden."""
    organize = {
        "id": "organize",
        "options": {
            "renameByName": columns,
            "indexByName": {name: i for i, name in enumerate(columns)},
            "excludeByName": {"Time": True, "__name__": True, "instance": True, "job": True,
                              "runner": True},
            "includeByName": {name: True for name in columns},
        },
    }
    return {
        "type": "table", "title": title, "description": description, "gridPos": pos, "datasource": DS,
        "targets": targets,
        "transformations": [{"id": "merge", "options": {}}, organize],
        "fieldConfig": {
            "defaults": {"custom": {"align": "auto",
                                    "cellOptions": {"type": "color-text" if cell_color else "auto"}},
                         "color": {"mode": "thresholds"}, "thresholds": steps("text")},
            "overrides": overrides or [],
        },
        "options": {"showHeader": True, "cellHeight": "sm",
                    "sortBy": [{"displayName": sort[0], "desc": sort[1]}] if sort else []},
    }


def col(name, **props):
    """Override for one table column: col("Value #B", unit="s", thresholds=...)."""
    mapping = {"unit": "unit", "thresholds": "thresholds", "mappings": "mappings",
               "decimals": "decimals", "width": "custom.width"}
    return {"matcher": {"id": "byName", "options": name},
            "properties": [{"id": mapping[k], "value": v} for k, v in props.items()]}


def text(title, content, pos):
    return {"type": "text", "title": title, "gridPos": pos,
            "options": {"mode": "markdown", "content": content}}


def row(title, y, collapsed=False):
    return {"type": "row", "title": title, "collapsed": collapsed, "panels": [],
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}}


def g(x, y, w, h):
    return {"x": x, "y": y, "w": w, "h": h}


def dashboard(uid, title, panels, refresh="1m", time_from="now-24h", tags=()):
    for i, p in enumerate(panels, start=1):
        p["id"] = i
    return {
        "uid": uid, "title": title, "tags": ["pokemon-momentum", *tags], "timezone": "browser",
        "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1,
        "refresh": refresh, "time": {"from": time_from, "to": "now"},
        "links": [{"title": "Pokemon Momentum", "type": "dashboards", "tags": ["pokemon-momentum"],
                   "asDropdown": False, "includeVars": False, "keepTime": True}],
        "panels": panels, "templating": {"list": []}, "annotations": {"list": []},
    }


# --- shared panels -------------------------------------------------------------------------


def jobs_table(pos):
    return table(
        "Did the jobs run?",
        [target(JOB_STATUS, ref="A", instant=True, table=True),
         target(f"time() - {LAST_SUCCESS}", ref="B", instant=True, table=True),
         target("max by (task) (momentum_job_last_exit_code)", ref="C", instant=True, table=True),
         target("max by (task) (momentum_job_last_duration_seconds)", ref="D", instant=True, table=True),
         target(INTERVAL, ref="E", instant=True, table=True),
         target("max by (task) (momentum_job_next_run_timestamp_seconds) * 1000", ref="F",
                instant=True, table=True)],
        pos,
        {"task": "Job", "Value #A": "Status", "Value #B": "Last success", "Value #C": "Exit code",
         "Value #D": "Took", "Value #E": "Runs every", "Value #F": "Next run (systemd)"},
        description="Every scheduled job (docs/operator_runbook.txt). Overdue = no success for "
                    "1.5 x its interval plus an hour. Cron jobs report through "
                    "scripts/monitoring/run_job.py, systemd jobs through collect_status.py.",
        overrides=[col("Value #A", mappings=JOB_STATUS_MAP, width=150),
                   col("Value #B", unit="dtdurations", decimals=0),
                   col("Value #C", thresholds=steps("green", (1, "red"))),
                   col("Value #D", unit="dtdurations", decimals=0),
                   col("Value #E", unit="dtdurations"),
                   col("Value #F", unit="dateTimeFromNow")],
        sort=("Status", True),
    )


def downloads_table(pos):
    return table(
        "Data downloads: how old",
        [target("time() - max by (source) (momentum_data_updated_timestamp_seconds)",
                ref="A", instant=True, table=True)],
        pos, {"source": "Source", "Value": "Age"},
        description="Time since each data source was last downloaded or rebuilt (file times "
                    "under data/). All are daily: orange past 1.5 days, red past 3.",
        overrides=[col("Value", unit="dtdurations", decimals=0,
                       thresholds=steps("green", (1.5 * DAY, "orange"), (3 * DAY, "red")))],
        sort=("Age", True),
    )


# --- dashboards ----------------------------------------------------------------------------


def overview():
    p = []
    p.append(row("Right now", 0))
    tiles = [
        ("Market site (public)", 'probe_success{instance="https://market.poke6s.com/health"}',
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="market.poke6s.com/health, checked every minute through the internet and Caddy.")),
        ("poke6s.com (public)", 'probe_success{instance="https://poke6s.com/"}',
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="The shop's public address (Squarespace until 2026-11-14).")),
        ("New storefront (local)", 'probe_success{instance="http://localhost:8010/healthz"}',
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="The new storefront container, checked on this host (not public yet).")),
        ("Price data age", PRICE_AGE,
         dict(unit="dtdurations", decimals=0, thresholds=steps("green", (2.2 * DAY, "orange"), (3.2 * DAY, "red")),
              description="Time since midnight of the newest price date. The 02:00 update adds the "
                          "previous day, so 1-2 days is normal.")),
        ("Jobs failed", JOBS_FAILED,
         dict(thresholds=steps("green", (1, "red")), description="Jobs whose last run exited with an error.")),
        ("Jobs overdue", JOBS_OVERDUE,
         dict(thresholds=steps("green", (1, "orange")), description="Jobs with no success for 1.5 x their interval + 1 h.")),
        ("Disk used (/)", ROOT_DISK_USED,
         dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (90, "red")))),
        ("Memory used", MEM_USED,
         dict(unit="percent", decimals=0, thresholds=steps("green", (85, "orange"), (95, "red")))),
    ]
    for i, (title, expr, kw) in enumerate(tiles):
        p.append(stat(title, expr, g(i * 3, 1, 3, 4), **kw))
    tiles2 = [
        ("Requests / min", "sum(rate(http_requests_total[5m])) * 60",
         dict(decimals=1, description="Both apps, all pages and API calls, last 5 minutes.")),
        ("Server errors (24 h)", 'sum(increase(http_requests_total{status=~"5.."}[24h])) or vector(0)',
         dict(decimals=0, thresholds=steps("green", (1, "orange"), (20, "red")),
              description="Responses with a 5xx status from either app.")),
        ("Market p95 response", 'histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{app="market"}[15m])))',
         dict(unit="s", decimals=2, thresholds=steps("green", (1, "orange"), (3, "red")),
              description="95% of market requests in the last 15 minutes were faster than this.")),
        ("New bug reports", 'sum(momentum_bug_reports{status="new"}) or vector(0)',
         dict(thresholds=steps("green", (1, "orange")), description="Discord #bug-reports items not yet handled (admin page /bug-reports).")),
        ("Todo: next", 'momentum_todo_items{status="next"}', dict(description="[next] items in docs/todo.txt.")),
        ("Todo: in progress", 'momentum_todo_items{status="in progress"}', dict(description="[in progress] items in docs/todo.txt.")),
        ("Uncommitted files", "momentum_git_uncommitted_files",
         dict(thresholds=steps("green", (1, "orange")), description="Changed project files not yet committed (data files left out). Also see unpushed commits on Jobs & work.")),
        ("Temporary sudo", "momentum_temporary_sudo_present",
         dict(mappings=value_map({1: ("REMOVE", "orange"), 0: ("Removed", "green")}),
              thresholds=steps("green", (1, "orange")),
              description="/etc/sudoers.d/90-claude-buildout gives adam password-free sudo for the monitoring build-out. "
                          "Remove it when done: sudo rm /etc/sudoers.d/90-claude-buildout")),
    ]
    for i, (title, expr, kw) in enumerate(tiles2):
        p.append(stat(title, expr, g(i * 3, 5, 3, 4), **kw))

    p.append(row("Jobs and data", 9))
    p.append(jobs_table(g(0, 10, 16, 10)))
    p.append(downloads_table(g(16, 10, 8, 10)))

    p.append(row("Trends", 20))
    p.append(timeseries("Requests / min by app",
                        [target("sum by (app) (rate(http_requests_total[5m])) * 60", "{{app}}")],
                        g(0, 21, 8, 8), unit="reqpm"))
    p.append(timeseries("Server CPU and memory used",
                        [target(CPU_USED, "CPU", "A"), target(MEM_USED, "Memory", "B")],
                        g(8, 21, 8, 8), unit="percent"))
    p.append(timeseries("Public probe response time",
                        [target('probe_duration_seconds{probe="public"}', "{{instance}}")],
                        g(16, 21, 8, 8), unit="s",
                        description="Full round trip from this host, out to the internet and back through Caddy."))
    return dashboard("momentum-overview", "Pokemon Momentum: Overview", p, tags=("overview",))


def website():
    p = []
    by_app = "sum by (app) (rate(http_requests_total[5m])) * 60"
    p.append(row("Traffic", 0))
    p.append(timeseries("Requests / min by app", [target(by_app, "{{app}}")], g(0, 1, 8, 8), unit="reqpm"))
    p.append(timeseries("Responses by status class",
                        [target('sum by (code) (label_replace(rate(http_requests_total[5m]), "code", "${1}xx", "status", "(.).*")) * 60', "{{code}}")],
                        g(8, 1, 8, 8), unit="reqpm",
                        description="2xx ok, 3xx redirects, 4xx not found or refused, 5xx server errors."))
    p.append(timeseries("Server error rate",
                        [target('100 * (sum by (app) (rate(http_requests_total{status=~"5.."}[5m])) or 0 * sum by (app) (rate(http_requests_total[5m])))'
                               ' / sum by (app) (rate(http_requests_total[5m]))', "{{app}}")],
                        g(16, 1, 8, 8), unit="percent", thresholds=steps("green", (1, "orange"), (5, "red"))))

    p.append(row("Speed", 9))
    for i, app in enumerate(("market", "storefront")):
        p.append(timeseries(f"Response time: {app}",
                            [target(f'histogram_quantile(0.5, sum by (le) (rate(http_request_duration_seconds_bucket{{app="{app}"}}[5m])))', "median", "A"),
                             target(f'histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{{app="{app}"}}[5m])))', "95th percentile", "B")],
                            g(i * 8, 10, 8, 8), unit="s"))
    p.append(timeseries("Requests being handled",
                        [target("http_requests_in_progress", "{{app}}")], g(16, 10, 8, 8),
                        description="More than a few at once means requests are queueing behind slow ones."))

    p.append(row("Routes (over the selected time range)", 18))
    p.append(table("Busiest routes",
                   [target('topk(25, sum by (app, route) (increase(http_requests_total[$__range])))', ref="A", instant=True, table=True)],
                   g(0, 19, 8, 12), {"app": "App", "route": "Route", "Value": "Requests"},
                   overrides=[col("Value", decimals=0)], sort=("Requests", True), cell_color=False))
    p.append(table("Slowest routes (95th percentile)",
                   [target('topk(25, histogram_quantile(0.95, sum by (app, route, le) (rate(http_request_duration_seconds_bucket[$__range]))))', ref="A", instant=True, table=True)],
                   g(8, 19, 8, 12), {"app": "App", "route": "Route", "Value": "p95"},
                   overrides=[col("Value", unit="s", decimals=2, thresholds=steps("text", (1, "orange"), (3, "red")))],
                   sort=("p95", True)))
    p.append(table("Errors by route",
                   [target('sum by (app, route, status) (increase(http_requests_total{status=~"[45].."}[$__range])) > 0', ref="A", instant=True, table=True)],
                   g(16, 19, 8, 12), {"app": "App", "route": "Route", "status": "Status", "Value": "Count"},
                   description="<unmatched> = addresses that match no route (mostly bots probing for files).",
                   overrides=[col("Value", decimals=0)], sort=("Count", True), cell_color=False))

    p.append(row("Outside checks", 31))
    p.append(table("Probes",
                   [target("probe_success", ref="A", instant=True, table=True),
                    target("probe_http_status_code", ref="B", instant=True, table=True),
                    target("probe_duration_seconds", ref="C", instant=True, table=True),
                    target("(probe_ssl_earliest_cert_expiry - time())", ref="D", instant=True, table=True)],
                   g(0, 32, 12, 7),
                   {"instance": "Address", "probe": "From", "Value #A": "Up", "Value #B": "HTTP",
                    "Value #C": "Took", "Value #D": "Certificate expires in"},
                   overrides=[col("Value #A", mappings=UP_DOWN),
                              col("Value #C", unit="s", decimals=2),
                              col("Value #D", unit="dtdurations", decimals=0,
                                  thresholds=steps("red", (7 * DAY, "orange"), (21 * DAY, "green")))]))
    p.append(timeseries("Probe response time",
                        [target("probe_duration_seconds", "{{instance}}")], g(12, 32, 12, 7), unit="s"))
    return dashboard("momentum-website", "Pokemon Momentum: Website", p, tags=("website",))


def servers():
    p = []
    p.append(row("This host (grafana, 192.168.1.240)", 0))
    tiles = [
        ("Uptime", "time() - node_boot_time_seconds", dict(unit="dtdurations", decimals=0)),
        ("CPU used", CPU_USED, dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (95, "red")))),
        ("Load (5 min) per core", 'node_load5 / scalar(count(node_cpu_seconds_total{mode="idle"}))',
         dict(decimals=2, thresholds=steps("green", (1, "orange"), (2, "red")),
              description="Above 1 means more work than cores.")),
        ("Memory used", MEM_USED, dict(unit="percent", decimals=0, thresholds=steps("green", (85, "orange"), (95, "red")))),
        ("Disk used (/)", ROOT_DISK_USED, dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (90, "red")))),
        ("Updates waiting", "sum(apt_upgrades_pending) or vector(0)", dict(thresholds=steps("green", (1, "orange")))),
        ("Reboot needed", "max(node_reboot_required) or vector(0)",
         dict(mappings=value_map({1: ("YES", "orange"), 0: ("No", "green")}), thresholds=steps("green", (1, "orange")))),
        # Only disks that really report SMART: this host is a Proxmox VM, and its virtual
        # disk (QEMU HARDDISK) has none, yet the SMART script still writes healthy=0 for it.
        ("Disks healthy (SMART)",
         "min(smartmon_device_smart_healthy and on (disk) (smartmon_device_smart_available == 1)) or vector(-1)",
         dict(mappings=value_map({1: ("OK", "green"), 0: ("FAILING", "red"), -1: ("No SMART (virtual disk)", "text")}),
              thresholds=steps("red", (1, "green")),
              description="smartctl's health verdict for disks that report SMART. This server is a VM, so its "
                          "disk is virtual and has none; the physical disks are on the Proxmox hosts.")),
    ]
    for i, (title, expr, kw) in enumerate(tiles):
        p.append(stat(title, expr, g(i * 3, 1, 3, 4), **kw))
    p.append(timeseries("CPU by mode",
                        [target('100 * sum by (mode) (rate(node_cpu_seconds_total{mode!="idle"}[5m])) / scalar(count(node_cpu_seconds_total{mode="idle"}))', "{{mode}}")],
                        g(0, 5, 8, 8), unit="percent", stack=True))
    p.append(timeseries("Memory",
                        [target("node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes", "used", "A"),
                         target("node_memory_Cached_bytes + node_memory_Buffers_bytes", "cache (reclaimable)", "B"),
                         target("node_memory_SwapTotal_bytes - node_memory_SwapFree_bytes", "swap used", "C")],
                        g(8, 5, 8, 8), unit="bytes"))
    p.append(table("Disk space",
                   [target('100 * (1 - node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs|rootfs|ramfs"} / node_filesystem_size_bytes)', ref="A", instant=True, table=True),
                    target('node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs|rootfs|ramfs"}', ref="B", instant=True, table=True),
                    target('node_filesystem_size_bytes{fstype!~"tmpfs|overlay|squashfs|rootfs|ramfs"}', ref="C", instant=True, table=True)],
                   g(16, 5, 8, 8),
                   {"mountpoint": "Mount", "Value #A": "Used", "Value #B": "Free", "Value #C": "Size"},
                   overrides=[col("Value #A", unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (90, "red"))),
                              col("Value #B", unit="bytes"), col("Value #C", unit="bytes")],
                   sort=("Used", True)))
    p.append(timeseries("Disk read and write",
                        [target('sum(rate(node_disk_read_bytes_total{device!~"loop.*|sr.*"}[5m]))', "read", "A"),
                         target('sum(rate(node_disk_written_bytes_total{device!~"loop.*|sr.*"}[5m]))', "write", "B")],
                        g(0, 13, 8, 8), unit="Bps"))
    p.append(timeseries("Network (main interface)",
                        [target('rate(node_network_receive_bytes_total{device="ens18"}[5m])', "in", "A"),
                         target('rate(node_network_transmit_bytes_total{device="ens18"}[5m])', "out", "B")],
                        g(8, 13, 8, 8), unit="Bps"))
    p.append(timeseries("Disk space used over time",
                        [target(ROOT_DISK_USED, "/")], g(16, 13, 8, 8), unit="percent",
                        thresholds=steps("green", (80, "orange"), (90, "red"))))

    p.append(row("Docker containers", 21))
    name = 'name=~".+"'
    p.append(table("Containers",
                   [target(f'sum by (name) (rate(container_cpu_usage_seconds_total{{{name}}}[5m])) * 100', ref="A", instant=True, table=True),
                    target(f'sum by (name) (container_memory_working_set_bytes{{{name}}})', ref="B", instant=True, table=True),
                    target(f'time() - max by (name) (container_start_time_seconds{{{name}}})', ref="C", instant=True, table=True)],
                   g(0, 22, 8, 12),
                   {"name": "Container", "Value #A": "CPU (% of a core)", "Value #B": "Memory", "Value #C": "Up for"},
                   description="Every container on this host, from cAdvisor. A short 'Up for' means it restarted.",
                   overrides=[col("Value #A", unit="percent", decimals=1), col("Value #B", unit="bytes"),
                              col("Value #C", unit="dtdurations", decimals=0, thresholds=steps("orange", (3600, "text")))],
                   sort=("Memory", True)))
    p.append(timeseries("Container CPU (% of a core)",
                        [target(f'topk(8, sum by (name) (rate(container_cpu_usage_seconds_total{{{name}}}[5m])) * 100)', "{{name}}")],
                        g(8, 22, 8, 12), unit="percent", description="The eight busiest containers."))
    p.append(timeseries("Container memory",
                        [target(f'topk(8, sum by (name) (container_memory_working_set_bytes{{{name}}}))', "{{name}}")],
                        g(16, 22, 8, 12), unit="bytes", description="The eight largest containers."))

    p.append(row("Monitoring itself", 34))
    p.append(table("Prometheus targets",
                   [target("up", ref="A", instant=True, table=True)], g(0, 35, 12, 8),
                   {"job": "Job", "instance": "Target", "Value": "Up"},
                   description="Everything Prometheus collects from. Proxmox (pve-exporter), UniFi (UnPoller) and the "
                               "machines in deploy/monitoring/nodes.yml feed the Home lab dashboard.",
                   overrides=[col("Value", mappings=UP_DOWN)], sort=("Up", False)))
    p.append(table("Status collector",
                   [target("momentum_collector_errors", ref="A", instant=True, table=True)], g(12, 35, 12, 8),
                   {"section": "Section of collect_status.py", "Value": "State"},
                   overrides=[col("Value", mappings=value_map({0: ("OK", "green"), 1: ("Failed", "red")}))]))
    # The table's excludeByName hides "job" by default; keep it for the targets table.
    p[-2]["transformations"][1]["options"]["excludeByName"].pop("job")
    p[-2]["transformations"][1]["options"]["excludeByName"].pop("instance")
    return dashboard("momentum-servers", "Pokemon Momentum: Servers", p, tags=("servers",))


def jobs_and_work():
    p = []
    p.append(row("Scheduled jobs", 0))
    p.append(jobs_table(g(0, 1, 16, 10)))
    p.append(downloads_table(g(16, 1, 8, 10)))
    p.append(timeseries("Job run time",
                        [target("max by (task) (momentum_job_last_duration_seconds)", "{{task}}")],
                        g(0, 11, 12, 8), unit="s",
                        description="How long each job's latest run took; steps change when a new run finishes."))
    p.append(table("Logs: last written",
                   [target("time() - momentum_log_updated_timestamp_seconds", ref="A", instant=True, table=True),
                    target("momentum_log_size_bytes", ref="B", instant=True, table=True)],
                   g(12, 11, 12, 8), {"log": "logs/<name>.log", "Value #A": "Last written", "Value #B": "Size"},
                   overrides=[col("Value #A", unit="dtdurations", decimals=0), col("Value #B", unit="bytes")],
                   sort=("Last written", False), cell_color=False))

    p.append(row("Work list and bug reports", 19))
    p.append({
        "type": "bargauge", "title": "docs/todo.txt by status", "gridPos": g(0, 20, 8, 9), "datasource": DS,
        "targets": [target("momentum_todo_items", "{{status}}", instant=True)],
        "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "blue"}, "min": 0}, "overrides": []},
        "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                    "valueMode": "text", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
    })
    tiles = [
        ("Todo last updated", "time() - momentum_todo_last_updated_timestamp_seconds",
         dict(unit="dtdurations", decimals=0, thresholds=steps("green", (7 * DAY, "orange")),
              description="From the 'Last updated' line in docs/todo.txt.")),
        ("New bug reports", 'sum(momentum_bug_reports{status="new"}) or vector(0)',
         dict(thresholds=steps("green", (1, "orange")))),
        ("Newest bug report", "time() - momentum_bug_report_newest_timestamp_seconds",
         dict(unit="dtdurations", decimals=0)),
        ("Site accounts", "momentum_site_accounts", dict()),
        ("Branch is main", "momentum_git_on_main",
         dict(mappings=value_map({1: ("main", "green"), 0: ("Other branch", "orange")}),
              thresholds=steps("orange", (1, "green")),
              description="The repo checkout in /opt/pokemon-momentum. Work branches are normal mid-task.")),
        ("Uncommitted files", "momentum_git_uncommitted_files", dict(thresholds=steps("green", (1, "orange")))),
        ("Unpushed commits", "momentum_git_unpushed_commits",
         dict(thresholds=steps("green", (1, "orange")), description="Commits on main not on origin/main, as of the last fetch.")),
        ("Last commit on main", "time() - momentum_git_last_commit_timestamp_seconds", dict(unit="dtdurations", decimals=0)),
    ]
    for i, (title, expr, kw) in enumerate(tiles):
        p.append(stat(title, expr, g(8 + (i % 4) * 4, 20 + (i // 4) * 4, 4, 4), **kw))
    p.append(text("Where these come from",
                  "Collected every minute by `scripts/monitoring/collect_status.py` (host cron). "
                  "Bug reports: Discord #bug-reports, copied into `tracking.sqlite3` every 15 minutes. "
                  "Setup and fixes: `docs/operator_runbook.txt`, *Monitoring*.",
                  g(8, 28, 16, 3)))
    return dashboard("momentum-jobs", "Pokemon Momentum: Jobs & work", p, tags=("jobs",))


# pve-exporter ids look like "node/pve", "qemu/100", "lxc/105", "storage/pve/local-lvm".
# Its *_info series carry the names; join them on id so tables show names, not numbers.
# The brackets around expr matter: "a - b * on (id) info" would join only b.
GUEST = 'id=~"(qemu|lxc)/.*"'
NODE = 'id=~"node/.*"'
STORAGE = 'id=~"storage/.*"'


def guest(expr):
    return f"max by (id, name, node, type) (({expr}) * on (id) group_left (name, node, type) pve_guest_info)"


def pve_node(expr):
    return f"max by (id, name) (({expr}) * on (id) group_left (name) pve_node_info)"


def pve_storage(expr):
    return f"max by (id, node, storage) (({expr}) * on (id) group_left (node, storage) pve_storage_info)"


def home_lab():
    """Machines outside this project's own host: the Proxmox cluster, the UniFi network (both
    since 2026-11-05) and any other machine listed in nodes.yml."""
    p = []
    p.append(row("Proxmox cluster (192.168.1.75)", 0))
    tiles = [
        ("Exporter can read Proxmox", 'max(up{job="proxmox"}) or vector(0)',
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="DOWN means the pve-exporter container can't read the Proxmox API "
                          "(token, permission or certificate; runbook 'Monitoring').")),
        ("Nodes up", f"count(pve_up{{{NODE}}} == 1) or vector(0)", dict()),
        ("Nodes down", f"count(pve_up{{{NODE}}} == 0) or vector(0)", dict(thresholds=steps("green", (1, "red")))),
        ("Guests running", f"count(pve_up{{{GUEST}}} == 1) or vector(0)",
         dict(description="Virtual machines and containers that are on.")),
        ("Guests stopped", f"count(pve_up{{{GUEST}}} == 0 unless on (id) pve_guest_info{{template=\"1\"}}) or vector(0)",
         dict(description="Off guests (templates left out). Not always a problem: some are off on purpose.")),
        ("Cluster CPU", f"100 * sum(pve_cpu_usage_ratio{{{NODE}}} * pve_cpu_usage_limit{{{NODE}}}) / sum(pve_cpu_usage_limit{{{NODE}}})",
         dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (95, "red")))),
        ("Cluster memory", f"100 * sum(pve_memory_usage_bytes{{{NODE}}}) / sum(pve_memory_size_bytes{{{NODE}}})",
         dict(unit="percent", decimals=0, thresholds=steps("green", (85, "orange"), (95, "red")))),
        ("Fullest storage", f"max(100 * pve_disk_usage_bytes{{{STORAGE}}} / (pve_disk_size_bytes{{{STORAGE}}} > 0))",
         dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (90, "red")),
              description="The most-used storage pool on any node; see the Storage table.")),
    ]
    for i, (title, expr, kw) in enumerate(tiles):
        p.append(stat(title, expr, g(i * 3, 1, 3, 4), **kw))

    p.append(table("Nodes",
                   [target(pve_node(f"pve_up{{{NODE}}}"), ref="A", instant=True, table=True),
                    target(pve_node(f"100 * pve_cpu_usage_ratio{{{NODE}}}"), ref="B", instant=True, table=True),
                    target(pve_node(f"100 * pve_memory_usage_bytes{{{NODE}}} / pve_memory_size_bytes{{{NODE}}}"), ref="C", instant=True, table=True),
                    target(pve_node(f"pve_uptime_seconds{{{NODE}}}"), ref="D", instant=True, table=True)],
                   g(0, 5, 8, 7),
                   {"name": "Node", "Value #A": "Up", "Value #B": "CPU", "Value #C": "Memory", "Value #D": "Up for"},
                   overrides=[col("Value #A", mappings=UP_DOWN),
                              col("Value #B", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (95, "red"))),
                              col("Value #C", unit="percent", decimals=0, thresholds=steps("text", (85, "orange"), (95, "red"))),
                              col("Value #D", unit="dtdurations", decimals=0)]))
    p.append(table("Storage",
                   [target(pve_storage(f"100 * pve_disk_usage_bytes{{{STORAGE}}} / (pve_disk_size_bytes{{{STORAGE}}} > 0)"), ref="A", instant=True, table=True),
                    target(pve_storage(f"pve_disk_size_bytes{{{STORAGE}}} - pve_disk_usage_bytes{{{STORAGE}}}"), ref="B", instant=True, table=True),
                    target(pve_storage(f"pve_disk_size_bytes{{{STORAGE}}}"), ref="C", instant=True, table=True)],
                   g(8, 5, 16, 7),
                   {"storage": "Storage", "node": "Node", "Value #A": "Used", "Value #B": "Free", "Value #C": "Size"},
                   overrides=[col("Value #A", unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (90, "red"))),
                              col("Value #B", unit="bytes"), col("Value #C", unit="bytes")],
                   sort=("Used", True)))
    p.append(table("Virtual machines and containers",
                   [target(guest(f"pve_up{{{GUEST}}}"), ref="A", instant=True, table=True),
                    target(guest(f"100 * pve_cpu_usage_ratio{{{GUEST}}}"), ref="B", instant=True, table=True),
                    target(guest(f"pve_memory_usage_bytes{{{GUEST}}}"), ref="C", instant=True, table=True),
                    target(guest(f"pve_memory_size_bytes{{{GUEST}}}"), ref="D", instant=True, table=True),
                    target(guest(f"pve_uptime_seconds{{{GUEST}}}"), ref="E", instant=True, table=True)],
                   g(0, 12, 24, 10),
                   {"name": "Name", "id": "ID", "type": "Type", "node": "Node", "Value #A": "Running",
                    "Value #B": "CPU (of its cores)", "Value #C": "Memory used", "Value #D": "Memory given",
                    "Value #E": "Up for"},
                   description="This VM (grafana, which runs the website) is one of these.",
                   overrides=[col("Value #A", mappings=value_map({1: ("Running", "green"), 0: ("Stopped", "text")})),
                              col("Value #B", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (95, "red"))),
                              col("Value #C", unit="bytes"), col("Value #D", unit="bytes"),
                              col("Value #E", unit="dtdurations", decimals=0)],
                   sort=("Memory used", True)))
    p.append(timeseries("Node CPU", [target(pve_node(f"100 * pve_cpu_usage_ratio{{{NODE}}}"), "{{name}}")],
                        g(0, 22, 8, 8), unit="percent"))
    p.append(timeseries("Busiest guests (CPU)",
                        [target(f"topk(8, {guest(f'100 * pve_cpu_usage_ratio{{{GUEST}}}')})", "{{name}}")],
                        g(8, 22, 8, 8), unit="percent", description="Percent of each guest's own cores."))
    p.append(timeseries("Largest guests (memory)",
                        [target(f"topk(8, {guest(f'pve_memory_usage_bytes{{{GUEST}}}')})", "{{name}}")],
                        g(16, 22, 8, 8), unit="bytes"))
    p.extend(network_row(30))
    p.extend(machines_row(56))
    return dashboard("momentum-homelab", "Pokemon Momentum: Home lab", p, tags=("homelab",))


# UnPoller series carry the device or client name; "uptime 0" is how it reports a device the
# controller has lost (an AP that is unplugged or offline).
UNIFI_STATUS_MAP = [{
    "type": "value",
    "options": {"ok": {"text": "ok", "color": "green", "index": 0},
                "warning": {"text": "warning", "color": "orange", "index": 1},
                "error": {"text": "error", "color": "red", "index": 2}},
}]


def machines_row(y):
    """Other machines listed in deploy/monitoring/nodes.yml (job "machines"): Linux and
    Synology run node-exporter, Windows runs windows_exporter. Empty until one is added."""
    m = 'job="machines"'
    by = "max by (instance, machine) "
    lin_cpu = f'100 * (1 - avg by (instance, machine) (rate(node_cpu_seconds_total{{{m},mode="idle"}}[5m])))'
    win_cpu = f'100 * (1 - avg by (instance, machine) (rate(windows_cpu_time_total{{{m},mode="idle"}}[5m])))'
    lin_mem = f'{by}(100 * (1 - node_memory_MemAvailable_bytes{{{m}}} / node_memory_MemTotal_bytes{{{m}}}))'
    win_mem = f'{by}(100 * (1 - windows_os_physical_memory_free_bytes{{{m}}} / windows_cs_physical_memory_bytes{{{m}}}))'
    lin_disk = (f'{by}(100 * (1 - node_filesystem_avail_bytes{{{m},fstype!~"tmpfs|overlay|squashfs|ramfs|rootfs"}}'
                f' / node_filesystem_size_bytes{{{m},fstype!~"tmpfs|overlay|squashfs|ramfs|rootfs"}}))')
    win_disk = f'{by}(100 * (1 - windows_logical_disk_free_bytes{{{m}}} / windows_logical_disk_size_bytes{{{m}}}))'
    return [
        row("Other machines (deploy/monitoring/nodes.yml)", y),
        table("Machines",
              [target(f"{by}(up{{{m}}})", ref="A", instant=True, table=True),
               target(f"({lin_cpu}) or ({win_cpu})", ref="B", instant=True, table=True),
               target(f"({lin_mem}) or ({win_mem})", ref="C", instant=True, table=True),
               target(f"({lin_disk}) or ({win_disk})", ref="D", instant=True, table=True),
               target(f"{by}(time() - node_boot_time_seconds{{{m}}}) or {by}(windows_system_system_up_time{{{m}}})",
                      ref="E", instant=True, table=True)],
              g(0, y + 1, 24, 8),
              {"machine": "Machine", "instance": "Address", "Value #A": "Up", "Value #B": "CPU",
               "Value #C": "Memory", "Value #D": "Fullest disk", "Value #E": "Up for"},
              description="Empty until a machine is added to deploy/monitoring/nodes.yml (runbook "
                          "'Monitoring', 'Adding another machine').",
              overrides=[col("Value #A", mappings=UP_DOWN),
                         col("Value #B", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (95, "red"))),
                         col("Value #C", unit="percent", decimals=0, thresholds=steps("text", (85, "orange"), (95, "red"))),
                         col("Value #D", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (90, "red"))),
                         col("Value #E", unit="dtdurations", decimals=0)]),
    ]


def network_row(y):
    """UniFi network, read by UnPoller (deploy/monitoring/docker-compose.yml)."""
    p = [row("Network (UniFi, Dream Machine Pro 192.168.1.1)", y)]
    y += 1
    tiles = [
        ("UnPoller can read UniFi", 'max(up{job="unifi"}) * (count(unpoller_device_info) > bool 0) or vector(0)',
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="DOWN means UnPoller can't sign in to the UniFi controller (account, password or "
                          "certificate; runbook 'Monitoring').")),
        ("Devices offline", "count(unpoller_device_uptime_seconds == 0) or vector(0)",
         dict(thresholds=steps("green", (1, "red")),
              description="UniFi devices the controller can't reach (uptime 0). See the Devices table.")),
        ("Internet latency", 'max(unpoller_site_latency_seconds{subsystem="www"})',
         dict(unit="s", decimals=3, thresholds=steps("green", (0.05, "orange"), (0.15, "red")))),
        ("Internet drops", 'max(unpoller_site_intenet_drops_total{subsystem="www"})',
         dict(thresholds=steps("green", (1, "orange"), (5, "red")),
              description="Internet outages the gateway counted (its own running total).")),
        ("Wired clients", 'sum(unpoller_site_users{subsystem="lan"})', dict()),
        ("Wi-Fi clients", 'sum(unpoller_site_users{subsystem="wlan"})', dict()),
        ("Firmware updates", "count(unpoller_device_upgradable == 1) or vector(0)",
         dict(thresholds=steps("green", (1, "orange")), description="Devices with newer firmware available.")),
        ("Gateway CPU", 'max(unpoller_device_cpu_utilization_ratio{type="udm"}) * 100',
         dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (95, "red")))),
    ]
    for i, (title, expr, kw) in enumerate(tiles):
        p.append(stat(title, expr, g(i * 3, y, 3, 4), **kw))
    y += 4

    p.append(table("Network areas (UniFi's own verdict)",
                   [target('count by (subsystem, status) ({__name__=~"unpoller_site_.+", subsystem!=""})',
                           ref="A", instant=True, table=True)],
                   g(0, y, 6, 8), {"subsystem": "Area", "status": "Status"},
                   description="wan = the internet link, www = reaching the internet, lan = switches, "
                               "wlan = Wi-Fi (warning when an AP is offline), vpn = VPN server.",
                   overrides=[col("status", mappings=UNIFI_STATUS_MAP)]))
    dev = "max by (name, type) "
    p.append(table("Devices",
                   [target("max by (name, type, model, ip, version) (unpoller_device_info)", ref="A", instant=True, table=True),
                    target(f"{dev}(unpoller_device_uptime_seconds > bool 0)", ref="B", instant=True, table=True),
                    target(f"{dev}(unpoller_device_uptime_seconds)", ref="C", instant=True, table=True),
                    target(f"{dev}(unpoller_device_stations)", ref="D", instant=True, table=True),
                    target(f"{dev}(unpoller_device_cpu_utilization_ratio) * 100", ref="E", instant=True, table=True),
                    target(f"{dev}(unpoller_device_temperature_celsius)", ref="F", instant=True, table=True),
                    target(f"{dev}(unpoller_device_upgradable)", ref="G", instant=True, table=True)],
                   g(6, y, 18, 8),
                   {"name": "Device", "type": "Type", "model": "Model", "ip": "IP", "Value #B": "Online",
                    "Value #C": "Up for", "Value #D": "Clients", "Value #E": "CPU", "Value #F": "Temp",
                    "Value #G": "Update", "version": "Firmware"},
                   description="uap = access point, usw = switch or PDU, udm = gateway.",
                   overrides=[
                              col("Value #B", mappings=value_map({1: ("Online", "green"), 0: ("OFFLINE", "red")})),
                              col("Value #C", unit="dtdurations", decimals=0),
                              col("Value #E", unit="percent", decimals=0),
                              col("Value #F", unit="celsius", decimals=0, thresholds=steps("text", (70, "orange"), (85, "red"))),
                              col("Value #G", mappings=value_map({1: ("available", "orange"), 0: ("", "text")}))],
                   sort=("Online", False)))
    y += 8

    p.append(timeseries("Internet traffic (gateway WAN)",
                        [target('sum(rate(unpoller_device_wan_receive_bytes_total[5m])) * 8', "download", "A"),
                         target('sum(rate(unpoller_device_wan_transmit_bytes_total[5m])) * 8', "upload", "B")],
                        g(0, y, 8, 8), unit="bps"))
    p.append(timeseries("Internet latency",
                        [target('max(unpoller_site_latency_seconds{subsystem="www"})', "latency")],
                        g(8, y, 8, 8), unit="s"))
    p.append(timeseries("Busiest clients (download)",
                        [target('topk(8, sum by (name) (rate(unpoller_client_receive_bytes_total[5m]))) * 8', "{{name}}")],
                        g(16, y, 8, 8), unit="bps",
                        description="Traffic each client received in the last 5 minutes, as UniFi counts it."))
    return p


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, board in (("overview", overview()), ("website", website()),
                        ("servers", servers()), ("jobs_and_work", jobs_and_work()),
                        ("home_lab", home_lab())):
        (OUT / f"{name}.json").write_text(json.dumps(board, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {OUT / f'{name}.json'} ({len(board['panels'])} panels)")


if __name__ == "__main__":
    main()
