#!/usr/bin/env python3
"""Build the Grafana dashboards for Pokemon Momentum as JSON files.

    python3 deploy/monitoring/build_dashboards.py

writes grafana/dashboards/*.json (Grafana folder "Pokemon Momentum") and grafana/homelab/*.json
(folder "Home lab") next to this file. Grafana loads both by itself (provisioning in
grafana/provisioning/dashboards.yml) within 30 s; no restart needed.
Change the dashboards here, not in the Grafana UI: UI edits are lost when the file is
rebuilt. The JSON is committed too, so the dashboards work without running this script.

Nine dashboards, all reading the Prometheus data source (uid "prometheus"). Pokemon Momentum:

* Overview: is everything all right? One screen of status tiles, the jobs table and the
  headline charts. (Grafana's home page is a cross-project "Everything" dashboard kept in a
  private repository; it falls back to this one if that isn't installed.) At the bottom, a collapsed "AIOS Experiments" row of
  placeholders (see AIOS_EXPERIMENTS below).
* Website: requests, errors and response times per app and route, and outside probes.
* Jobs & work: scheduled jobs, data downloads, logs, docs/todo.txt, bug reports, git.
* Visitors: people and page views per day, top pages, referrers and devices, per site.
* Store: the new storefront's orders, sales and stock (live orders by default; test on request).

Home lab (moved to their own folder 2026-11-05, as they serve more than the Pokemon sites):

* Proxmox cluster: nodes, VMs, containers, storage.
* Network: internet uptime from this server's own probes, then UniFi devices, clients, WAN.
* Servers: this host (CPU, memory, disks, network, updates), every Docker container,
  monitoring itself, and other machines listed in nodes.yml.

Where the numbers come from: docs/operator_runbook.txt, "Monitoring".

Design rules (from the team's chart guidance): stat tiles for headline numbers; green /
orange / red only for status, always with the value or a word beside it; thin lines; one
y-axis per chart.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "grafana" / "dashboards"      # folder "Pokemon Momentum"
OUT_HOMELAB = Path(__file__).resolve().parent / "grafana" / "homelab"  # folder "Home lab"
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


def dashboard(uid, title, panels, refresh="1m", time_from="now-24h", tags=(), family="pokemon-momentum"):
    """family is the tag shared by one Grafana folder's dashboards ("pokemon-momentum" or
    "homelab"); the top bar of every dashboard has a drop-down for each family."""
    next_id = 1
    for p in panels:
        # A collapsed row keeps its panels inside it; they need ids too.
        for panel in [p, *p.get("panels", [])]:
            panel["id"] = next_id
            next_id += 1
    return {
        "uid": uid, "title": title, "tags": [family, *tags], "timezone": "browser",
        "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1,
        "refresh": refresh, "time": {"from": time_from, "to": "now"},
        "links": [{"title": "Home", "type": "link", "url": "/", "icon": "dashboard", "targetBlank": False}] +
                 [{"title": name, "type": "dashboards", "tags": [tag],
                   "asDropdown": True, "includeVars": False, "keepTime": True}
                  for name, tag in (("Pokemon Momentum", "pokemon-momentum"), ("Home lab", "homelab"))],
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
        ("Visitors today", 'sum(site_today_total{kind="visitors"})',
         dict(decimals=0, link=("Visitors dashboard", "/d/momentum-visitors"),
              description="People who loaded a page on market.poke6s.com or the new storefront today "
                          "(UTC), bots left out. Details on the Visitors dashboard.")),
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
    if AIOS_EXPERIMENTS:
        p.append(aios_experiments_row(29))
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
                               "machines in deploy/monitoring/nodes.yml feed 'Other machines' below.",
                   overrides=[col("Value", mappings=UP_DOWN)], sort=("Up", False)))
    p.append(table("Status collector",
                   [target("momentum_collector_errors", ref="A", instant=True, table=True)], g(12, 35, 12, 8),
                   {"section": "Section of collect_status.py", "Value": "State"},
                   overrides=[col("Value", mappings=value_map({0: ("OK", "green"), 1: ("Failed", "red")}))]))
    # The table's excludeByName hides "job" by default; keep it for the targets table.
    p[-2]["transformations"][1]["options"]["excludeByName"].pop("job")
    p[-2]["transformations"][1]["options"]["excludeByName"].pop("instance")
    p.extend(machines_row(43))
    p.append(zfs_pools_table(g(0, 52, 24, 4)))
    p.append(smart_table(g(0, 56, 24, 9)))
    p.extend(plex_row(65))
    p.extend(nas_row(74,'job="synology"', "Synology NAS (SNMP; deploy/monitoring/prometheus_momentum.yml, job synology)"))
    return dashboard("momentum-servers", "Home lab: Servers", p, tags=("servers",), family="homelab")


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


def proxmox():
    """The Proxmox cluster (since 2026-11-05). Keeps the old "Home lab" uid so links still work."""
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
    # Proxmox servers outside the cluster API (the Backup Server): node-exporter, labelled
    # group: proxmox in deploy/monitoring/nodes.yml.
    p.append(row("Proxmox Backup Server", 30))
    p.extend(pbs_panels(31))
    return dashboard("momentum-homelab", "Home lab: Proxmox cluster", p, tags=("proxmox",), family="homelab")


# PBS backups come from the pbs section of scripts/monitoring/collect_status.py (API token
# with the Audit role; runbook "Proxmox Backup Server"). Guests are matched to the Proxmox
# exporter's guests by VMID: PBS calls them vm/101 or ct/101, pve-exporter qemu/101 or lxc/101.
PBS_GROUP = "momentum_pbs_group_last_backup_timestamp_seconds"
PBS_VERIFY_MAP = value_map({1: ("OK", "green"), 0: ("FAILED", "red"), 2: ("Not verified yet", "text")})


def pve_guests(templates=True):
    """pve_guest_info relabelled to PBS's naming (type vm/ct, id 101) so it joins on (type, id).
    Matching the type too matters: a VMID freed by a deleted container can come back as a VM,
    and PBS keeps the old ct/101 group next to the new vm/101 one."""
    sel = "" if templates else '{template!="1"}'
    expr = f'label_replace(pve_guest_info{sel}, "id", "$2", "id", "(qemu|lxc)/(.*)")'
    expr = f'label_replace(label_replace({expr}, "type", "vm", "type", "qemu"), "type", "ct", "type", "lxc")'
    return f"max by (type, id, name) ({expr})"


def pbs_named(expr):
    """Add the Proxmox guest name (label name) to a per-backup-group series. Groups whose guest
    no longer exists keep no name, which is how the table shows them."""
    return (f"max by (store, type, id, guest, name) (({expr}) * on (type, id) group_left (name) {pve_guests()} "
            f"or (({expr}) unless on (type, id) {pve_guests()}))")


def pbs_panels(y):
    stale = 26 * 3600  # daily backups, plus a couple of hours' grace
    tiles = [
        ("PBS API", "max(momentum_pbs_up) or vector(0)",
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="DOWN: no token in /etc/momentum-monitoring/pbs-token, or the collector "
                          "couldn't read PBS (Status collector table on Home lab: Servers).")),
        # Only guests that still exist (templates left out), each by its newest backup in any
        # datastore: PBS also holds old groups of deleted guests and older copies in retired
        # datastores, which would otherwise pin this tile at months or years.
        ("Stalest backup", f"time() - min(max by (type, id) ({PBS_GROUP}) and on (type, id) "
                           f"{pve_guests(templates=False)})",
         dict(unit="s", decimals=0, thresholds=steps("green", (stale, "orange"), (2 * stale, "red")),
              description="Over the cluster's current guests (templates left out), the age of the "
                          "oldest 'newest backup', any datastore. Over a day means a guest missed its "
                          "backup, or isn't in a backup job (see Backups by guest).")),
        ("Backup failures (7 days)", 'sum(momentum_pbs_task_failures_7d{type="backup"}) or vector(0)',
         dict(thresholds=steps("green", (1, "red")))),
        ("Other task failures (7 days)", 'sum(momentum_pbs_task_failures_7d{type!="backup"}) or vector(0)',
         dict(thresholds=steps("green", (1, "red")),
              description="Verify, prune, garbage collection and sync tasks (see Tasks).")),
        ("Verify failed", "count(momentum_pbs_group_last_verify == 0) or vector(0)",
         dict(thresholds=steps("green", (1, "red")),
              description="Guests whose newest backup failed verification: that backup may not restore.")),
        ("Guests with no backup",
         f"count({pve_guests(templates=False)} unless on (type, id) {PBS_GROUP}) or vector(0)",
         dict(thresholds=steps("green", (1, "orange")),
              description="Cluster guests (templates left out) with no backup group on PBS. Some may be "
                          "left out on purpose.")),
        ("Datastore used", "max(100 * momentum_pbs_datastore_bytes{kind=\"used\"} / ignoring (kind) "
                           "momentum_pbs_datastore_bytes{kind=\"total\"})",
         dict(unit="percent", decimals=0, thresholds=steps("green", (80, "orange"), (90, "red")))),
    ]
    p = [stat(t, e, g(i * 3, y, 3, 4), **kw) for i, (t, e, kw) in enumerate(tiles)]
    p.append(machines_table("Server", 'job="machines",group="proxmox"', g(0, y + 4, 24, 4),
                            "node-exporter on the Backup Server (deploy/monitoring/nodes.yml, "
                            "group: proxmox)."))
    p.append(table("Backups by guest",
                   [target(pbs_named(f"time() - {PBS_GROUP}"), ref="A", instant=True, table=True),
                    target(pbs_named("momentum_pbs_group_snapshots"), ref="B", instant=True, table=True),
                    target(pbs_named("momentum_pbs_group_last_verify"), ref="C", instant=True, table=True)],
                   g(0, y + 8, 14, 10),
                   {"name": "Guest", "guest": "Backup note", "type": "Type", "id": "ID", "store": "Datastore",
                    "Value #A": "Newest backup", "Value #B": "Backups kept", "Value #C": "Newest verified"},
                   description="One row per backup group on PBS. Guest is the name in Proxmox; a blank "
                               "Guest is a backup of a guest that no longer exists (or whose VMID now "
                               "belongs to a guest of the other type). Old groups in a datastore no "
                               "longer backed up to stay until pruned by hand.",
                   overrides=[col("Value #A", unit="s", decimals=0,
                                  thresholds=steps("text", (stale, "orange"), (2 * stale, "red"))),
                              col("Value #C", mappings=PBS_VERIFY_MAP)],
                   sort=("Newest backup", True)))
    p.append(table("Tasks (last 7 days)",
                   [target("max by (type) (time() - momentum_pbs_task_last_end_timestamp_seconds)",
                           ref="A", instant=True, table=True),
                    target("max by (type) (momentum_pbs_task_last_ok)", ref="B", instant=True, table=True),
                    target("max by (type) (momentum_pbs_task_failures_7d)", ref="C", instant=True, table=True)],
                   g(14, y + 8, 10, 6),
                   {"type": "Task", "Value #A": "Last finished", "Value #B": "Last result", "Value #C": "Failures"},
                   description="Types with no run in 7 days show no 'Last finished'. *job types are the "
                               "scheduled verify and prune jobs.",
                   overrides=[col("Value #A", unit="s", decimals=0),
                              col("Value #B", mappings=value_map({1: ("OK", "green"), 0: ("FAILED", "red")})),
                              col("Value #C", thresholds=steps("text", (1, "red")))]))
    p.append(table("Datastores",
                   [target('max by (store) (100 * momentum_pbs_datastore_bytes{kind="used"} / ignoring (kind) '
                           'momentum_pbs_datastore_bytes{kind="total"})', ref="A", instant=True, table=True),
                    target('max by (store) (momentum_pbs_datastore_bytes{kind="avail"})', ref="B", instant=True, table=True),
                    target('max by (store) (momentum_pbs_datastore_bytes{kind="total"})', ref="C", instant=True, table=True)],
                   g(14, y + 14, 10, 4),
                   {"store": "Datastore", "Value #A": "Used", "Value #B": "Free", "Value #C": "Size"},
                   overrides=[col("Value #A", unit="percent", decimals=0,
                                  thresholds=steps("text", (80, "orange"), (90, "red"))),
                              col("Value #B", unit="bytes"), col("Value #C", unit="bytes")]))
    return p


def network():
    p = internet_row(0)
    p += network_row(13)
    return dashboard("homelab-network", "Home lab: Network", p, tags=("network",), family="homelab")


# Internet and ISP uptime, from this server's own probes (job "internet" in
# prometheus_momentum.yml): TCP connects every 30 s to the router and to two outside addresses.
INTERNET_UP = 'max(probe_success{job="internet",side="wan"})'  # either outside address answered
ROUTER_UP = 'max(probe_success{job="internet",side="lan"})'


def state_timeline(title, targets, pos, mappings, description=""):
    return {
        "type": "state-timeline", "title": title, "gridPos": pos, "datasource": DS, "description": description,
        "targets": targets,
        "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "mappings": mappings,
                                     "thresholds": steps("red", (1, "green")),
                                     "custom": {"fillOpacity": 80, "lineWidth": 0}},
                        "overrides": []},
        "options": {"showValue": "never", "mergeValues": True, "rowHeight": 0.8, "alignValue": "left",
                    "legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
    }


def internet_row(y):
    p = [row("Internet uptime (probes from this server every 30 s)", y)]
    uptime = lambda rng: f"100 * avg_over_time(({INTERNET_UP})[{rng}:30s])"
    tiles = [
        ("Internet now", f"{INTERNET_UP} or vector(0)",
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="UP when Cloudflare (1.1.1.1) or Google (8.8.8.8) answered the last check.")),
        ("Router now", f"{ROUTER_UP} or vector(0)",
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="The Dream Machine (192.168.1.1). If this is down too, the problem is inside the house.")),
        ("Uptime, 24 h", uptime("24h"),
         dict(unit="percent", decimals=2, thresholds=steps("red", (99, "orange"), (99.9, "green")))),
        ("Uptime, 7 days", uptime("7d"),
         dict(unit="percent", decimals=2, thresholds=steps("red", (99, "orange"), (99.9, "green")))),
        ("Uptime, 30 days", uptime("30d"),
         dict(unit="percent", decimals=2, thresholds=steps("red", (99, "orange"), (99.9, "green")),
              description="Counted from 2026-11-09, when these probes started.")),
        ("Down time, 7 days", f"0.5 * sum_over_time(({INTERNET_UP} == bool 0)[7d:30s])",
         dict(unit="m", decimals=1, thresholds=steps("green", (1, "orange"), (30, "red")),
              description="Minutes with no answer from either outside address, in 30-second steps.")),
        ("Latency (Cloudflare)", 'probe_duration_seconds{job="internet",instance="1.1.1.1:443"}',
         dict(unit="s", decimals=3, thresholds=steps("green", (0.05, "orange"), (0.15, "red")),
              description="Time to open a connection to 1.1.1.1 just now (one round trip plus a little).")),
        ("Latency (router)", 'probe_duration_seconds{job="internet",side="lan"}',
         dict(unit="s", decimals=4, thresholds=steps("green", (0.01, "orange"), (0.05, "red")))),
    ]
    p += [stat(t, e, g(i * 3, y + 1, 3, 4), **kw) for i, (t, e, kw) in enumerate(tiles)]
    p.append(state_timeline("Up or down",
                            [target(INTERNET_UP, "Internet", "A"), target(ROUTER_UP, "Router", "B")],
                            g(0, y + 5, 12, 8), UP_DOWN,
                            description="Red stretches are outages. Internet red with Router green: the ISP "
                                        "or the modem. Both red: this server or the house network."))
    p.append(timeseries("Connection time",
                        [target('probe_duration_seconds{job="internet"}', "{{name}}")],
                        g(12, y + 5, 12, 8), unit="s",
                        description="Time to open a TCP connection, every 30 s. Spikes mean a busy or "
                                    "struggling line."))
    return p


# UnPoller series carry the device or client name; "uptime 0" is how it reports a device the
# controller has lost (an AP that is unplugged or offline).
UNIFI_STATUS_MAP = [{
    "type": "value",
    "options": {"ok": {"text": "ok", "color": "green", "index": 0},
                "warning": {"text": "warning", "color": "orange", "index": 1},
                "error": {"text": "error", "color": "red", "index": 2}},
}]


def machines_table(title, m, pos, description):
    """One row per machine scraped by job "machines" (deploy/monitoring/nodes.yml) that matches
    the label selector m: Linux and Synology run node-exporter, Windows runs windows_exporter."""
    by = "max by (instance, machine, role) "
    lin_cpu = f'100 * (1 - avg by (instance, machine, role) (rate(node_cpu_seconds_total{{{m},mode="idle"}}[5m])))'
    win_cpu = f'100 * (1 - avg by (instance, machine, role) (rate(windows_cpu_time_total{{{m},mode="idle"}}[5m])))'
    lin_mem = f'{by}(100 * (1 - node_memory_MemAvailable_bytes{{{m}}} / node_memory_MemTotal_bytes{{{m}}}))'
    win_mem = f'{by}(100 * (1 - windows_os_physical_memory_free_bytes{{{m}}} / windows_cs_physical_memory_bytes{{{m}}}))'
    lin_disk = (f'{by}(100 * (1 - node_filesystem_avail_bytes{{{m},fstype!~"tmpfs|overlay|squashfs|ramfs|rootfs"}}'
                f' / node_filesystem_size_bytes{{{m},fstype!~"tmpfs|overlay|squashfs|ramfs|rootfs"}}))')
    win_disk = f'{by}(100 * (1 - windows_logical_disk_free_bytes{{{m}}} / windows_logical_disk_size_bytes{{{m}}}))'
    return table(title,
              [target(f"{by}(up{{{m}}})", ref="A", instant=True, table=True),
               target(f"({lin_cpu}) or ({win_cpu})", ref="B", instant=True, table=True),
               target(f"({lin_mem}) or ({win_mem})", ref="C", instant=True, table=True),
               target(f"({lin_disk}) or ({win_disk})", ref="D", instant=True, table=True),
               target(f"{by}(time() - node_boot_time_seconds{{{m}}}) or {by}(windows_system_system_up_time{{{m}}})",
                      ref="E", instant=True, table=True)],
              pos,
              {"machine": "Machine", "role": "Role", "instance": "Address", "Value #A": "Up", "Value #B": "CPU",
               "Value #C": "Memory", "Value #D": "Fullest disk", "Value #E": "Up for"},
              description=description,
              overrides=[col("Value #A", mappings=UP_DOWN),
                         col("Value #B", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (95, "red"))),
                         col("Value #C", unit="percent", decimals=0, thresholds=steps("text", (85, "orange"), (95, "red"))),
                         col("Value #D", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (90, "red"))),
                         col("Value #E", unit="dtdurations", decimals=0)])


def machines_row(y):
    """Other machines listed in deploy/monitoring/nodes.yml, except those labelled
    group: proxmox (the Proxmox Backup Server), which the Proxmox cluster dashboard shows."""
    return [
        row("Other machines (deploy/monitoring/nodes.yml)", y),
        machines_table("Machines", 'job="machines",group!="proxmox"', g(0, y + 1, 17, 8),
                       "Empty until a machine is added to deploy/monitoring/nodes.yml (runbook "
                       "'Monitoring', 'Adding another machine'). Proxmox machines are on the "
                       "Proxmox cluster dashboard."),
        table("Home lab services",
              [target('probe_success{probe="homelab"}', ref="A", instant=True, table=True),
               target('probe_duration_seconds{probe="homelab"}', ref="B", instant=True, table=True)],
              g(17, y + 1, 7, 8),
              {"service": "Service", "Value #A": "Up", "Value #B": "Response"},
              description="Web checks of home lab services (blackbox-http job, probe=homelab in "
                          "deploy/monitoring/prometheus_momentum.yml). Plex: its /identity page.",
              overrides=[col("Value #A", mappings=UP_DOWN), col("Value #B", unit="s", decimals=2)]),
    ]


# ZFS pool health (node-exporter's zfs collector, 1.5+): node_zfs_zpool_state is 1 for the
# pool's current state (label state) and 0 for the others.
ZPOOL_STATE_MAP = [{
    "type": "value",
    "options": {"online": {"text": "Online", "color": "green", "index": 0},
                "degraded": {"text": "Degraded (a disk is missing or failed)", "color": "red", "index": 1},
                "faulted": {"text": "Faulted", "color": "red", "index": 2},
                "offline": {"text": "Offline", "color": "red", "index": 3},
                "removed": {"text": "Removed", "color": "red", "index": 4},
                "unavail": {"text": "Unavailable", "color": "red", "index": 5},
                "suspended": {"text": "Suspended", "color": "red", "index": 6}},
}]


def smart_named(expr):
    """Per-disk SMART series with the disk's model and serial added (from smartmon_device_info),
    limited to disks that really report SMART (a VM's virtual disk doesn't). The host itself
    is scraped as localhost with no machine label; it shows as "grafana"."""
    info = ("max by (instance, disk, device_model, serial_number) (smartmon_device_info) "
            "and on (instance, disk) (smartmon_device_smart_available == 1)")
    joined = f"max by (instance, disk) ({expr}) * on (instance, disk) group_left (device_model, serial_number) ({info})"
    machine = "max by (instance, machine) (label_replace(smartmon_smartctl_version, \"machine\", \"grafana\", \"machine\", \"\"))"
    return (f"max by (machine, disk, device_model, serial_number) "
            f"(({joined}) * on (instance) group_left (machine) {machine})")


def smart_table(pos):
    """SMART for every disk of every machine running the smartmon collector (the Debian package
    prometheus-node-exporter-collectors with smartmontools; deploy/monitoring/smart/). The
    verdict ("healthy") only flips when a disk is nearly dead, so the counters beside it are
    what matter: any pending, uncorrectable or reallocated sector means the disk is failing."""
    cols = [
        ("A", "smartmon_device_smart_healthy", "Verdict",
         dict(mappings=value_map({1: ("PASSED", "text"), 0: ("FAILING", "red")}))),
        ("B", "smartmon_current_pending_sector_raw_value", "Pending sectors", dict(thresholds=steps("text", (1, "red")))),
        ("C", "smartmon_offline_uncorrectable_raw_value", "Uncorrectable", dict(thresholds=steps("text", (1, "red")))),
        ("D", "smartmon_reallocated_sector_ct_raw_value", "Reallocated", dict(thresholds=steps("text", (1, "orange"), (50, "red")))),
        # A lifetime total, often large from one old cable fault; only new errors matter.
        ("E", "delta(smartmon_udma_crc_error_count_raw_value[7d])", "Cable errors (7 days)",
         dict(decimals=0, thresholds=steps("text", (1, "orange")))),
        ("F", "smartmon_temperature_celsius_raw_value", "Temp",
         dict(unit="celsius", thresholds=steps("text", (45, "orange"), (55, "red")))),
        ("G", "smartmon_power_on_hours_raw_value / 8766", "Age (powered on)", dict(unit="none", decimals=1)),
    ]
    return table("Disk health (SMART)",
                 [target(smart_named(expr), ref=ref, instant=True, table=True) for ref, expr, _, _ in cols],
                 pos,
                 {"machine": "Machine", "disk": "Disk", "device_model": "Model", "serial_number": "Serial",
                  **{f"Value #{ref}": head for ref, _, head, _ in cols}},
                 description="One row per physical disk, from smartctl every 15 minutes. Any pending or "
                             "uncorrectable sector means the disk has unreadable spots and is failing: "
                             "plan to replace it (it may still say PASSED). Cable errors that keep appearing "
                             "usually mean a loose or bad SATA cable, not a bad disk. Age is in years of power-on time. "
                             "Adding a machine: runbook 'Monitoring', 'SMART'.",
                 overrides=[col(f"Value #{ref}", **kw) for ref, _, _, kw in cols],
                 sort=("Pending sectors", True))


def zfs_pools_table(pos):
    """ZFS pools on the machines in nodes.yml (the OpenMediaVault server's pool "zfs").
    State comes from node-exporter's zfs collector; the rest from deploy/monitoring/zfs/
    zfs_status.py on the machine (zpool list and zpool status, every 5 minutes). Those
    columns stay empty until that script is installed there."""
    m = 'job="machines"'
    by = "max by (machine, zpool) "
    return table("ZFS pools (RAID health)",
                 [target(f"max by (machine, zpool, state) (node_zfs_zpool_state{{{m}}} == 1)",
                         ref="A", instant=True, table=True),
                  target(f"{by}(100 * zfs_pool_allocated_bytes{{{m}}} / zfs_pool_size_bytes{{{m}}})",
                         ref="B", instant=True, table=True),
                  target(f"{by}(zfs_pool_free_bytes{{{m}}})", ref="C", instant=True, table=True),
                  target(f"{by}(time() - zfs_pool_scrub_last_end_timestamp_seconds{{{m}}})",
                         ref="D", instant=True, table=True),
                  target(f"{by}(zfs_pool_scan_in_progress{{{m}}})", ref="E", instant=True, table=True),
                  target(f"{by}(zfs_pool_scrub_last_errors{{{m}}})", ref="F", instant=True, table=True),
                  target(f"sum by (machine, zpool) (zfs_pool_device_errors{{{m}}})", ref="G", instant=True, table=True),
                  target(f"sum by (machine, zpool) (1 - zfs_pool_device_online{{{m}}})", ref="H", instant=True, table=True),
                  target(f"{by}(zfs_pool_data_errors{{{m}}})", ref="I", instant=True, table=True)],
                 pos,
                 {"machine": "Machine", "zpool": "Pool", "state": "State", "Value #B": "Used", "Value #C": "Free",
                  "Value #D": "Last scrub", "Value #E": "Scrub/resilver running", "Value #F": "Scrub errors",
                  "Value #G": "Disk errors", "Value #H": "Disks not online", "Value #I": "Data errors"},
                 description="Degraded: the pool runs without one of its disks; replace it soon. Disk errors "
                             "are read, write and checksum counters summed over every disk: any above 0 "
                             "deserves `zpool status -v` on the server. Scrubs should run at least monthly. "
                             "Data errors are files ZFS couldn't repair.",
                 overrides=[col("state", mappings=ZPOOL_STATE_MAP),
                            col("Value #B", unit="percent", decimals=0,
                                thresholds=steps("text", (80, "orange"), (90, "red"))),
                            col("Value #C", unit="bytes"),
                            col("Value #D", unit="s", decimals=0,
                                thresholds=steps("text", (35 * 86400, "orange"), (60 * 86400, "red"))),
                            col("Value #E", mappings=value_map({1: ("Yes", "orange"), 0: ("No", "text")})),
                            col("Value #F", thresholds=steps("text", (1, "red"))),
                            col("Value #G", thresholds=steps("text", (1, "red"))),
                            col("Value #H", thresholds=steps("text", (1, "red"))),
                            col("Value #I", thresholds=steps("text", (1, "red")))])


def plex_row(y):
    """Plex (192.168.1.219): streams and libraries, from the plex section of
    scripts/monitoring/collect_status.py (needs the owner's token file; runbook "Plex")."""
    streams = 'sum(momentum_plex_streams) or vector(0)'
    return [
        row("Plex (192.168.1.219)", y),
        stat("Streams now", streams, g(0, y + 1, 4, 4),
             description="Everyone watching or listening, paused streams included."),
        stat("Transcoding", "momentum_plex_transcodes or vector(0)", g(0, y + 5, 4, 4),
             thresholds=steps("text", (3, "orange")),
             description="Streams the server converts on the fly (the rest play directly). "
                         "Each one costs CPU on the Plex machine."),
        table("Libraries",
              [target("max by (library, type) (momentum_plex_library_items)", ref="A", instant=True, table=True),
               target('max by (library, type) (label_replace(momentum_plex_library_episodes, "type", "show", "", ""))',
                      ref="B", instant=True, table=True)],
              g(4, y + 1, 9, 8),
              {"library": "Library", "type": "Kind", "Value #A": "Items", "Value #B": "Episodes"},
              description="Items: movies, shows or artists, as Plex counts them. Episodes: TV libraries only.",
              sort=("Items", True)),
        timeseries("Streams", [target("sum(momentum_plex_streams)", "streams"),
                               target("momentum_plex_transcodes", "transcoding", ref="B")],
                   g(13, y + 1, 11, 8)),
    ]


def nas_table(title, sel, pos, description=""):
    """One row per Synology NAS (label nas), from the snmp-exporter's synology module.
    Status codes (Synology MIB): systemStatus 1 normal / 2 failed; raidStatus 1 normal,
    11 degraded, 12 crashed, others are busy states (sync, scrub, expand); diskStatus 1 normal;
    diskHealthStatus 1 normal; upgradeAvailable 1 available / 2 up to date. A copy of this
    lives in the owner's private ISI dashboards (same metrics, other NAS units)."""
    # Every column carries the same labels (nas, model, DSM version) so the table merges into
    # one row per NAS.
    info = (f"(max by (nas, modelName) (modelName{{{sel}}}) * on (nas) group_left (version) "
            f"max by (nas, version) (version{{{sel}}}))")
    lab = lambda e: f"({e}) * on (nas) group_left (modelName, version) {info}"
    zero = f"max by (nas) (systemStatus{{{sel}}}) * 0"
    disks_bad = (f"count by (nas) ((max by (nas, diskID) (diskStatus{{{sel}}} != bool 1) + "
                 f"max by (nas, diskID) (diskHealthStatus{{{sel}}} != bool 1)) > 0)")
    cols = [
        ("A", f"max by (nas) (systemStatus{{{sel}}})"),
        ("B", f"max by (nas) (100 * (1 - raidFreeSize{{{sel}}} / raidTotalSize{{{sel}}}))"),
        ("C", f"count by (nas) (raidStatus{{{sel}}} != 1) or {zero}"),
        ("D", f"({disks_bad}) or {zero}"),
        ("E", f"max by (nas) (temperature{{{sel}}})"),
        ("F", f"max by (nas) (diskTemperature{{{sel}}})"),
        ("G", f"max by (nas) (upgradeAvailable{{{sel}}})"),
        ("H", f"max by (nas) (100 - ssCpuIdle{{{sel}}})"),
        ("I", f"max by (nas) (100 * (1 - (memAvailReal{{{sel}}} + memBuffer{{{sel}}} + memCached{{{sel}}}) / memTotalReal{{{sel}}}))"),
        ("J", f"max by (nas) (hrSystemUptime{{{sel}}}) / 100"),  # system uptime (sysUpTime restarts with SNMP)
    ]
    return table(title, [target(lab(e), ref=r, instant=True, table=True) for r, e in cols], pos,
                 {"nas": "NAS", "modelName": "Model", "version": "DSM", "Value #A": "System",
                  "Value #B": "Fullest volume", "Value #C": "Volumes not normal", "Value #D": "Disks not normal",
                  "Value #E": "System temp", "Value #F": "Hottest disk", "Value #G": "DSM update",
                  "Value #H": "CPU", "Value #I": "Memory", "Value #J": "Up for"},
                 description=description,
                 overrides=[col("Value #A", mappings=value_map({1: ("Normal", "green"), 2: ("FAILED", "red")})),
                            col("Value #B", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (90, "red"))),
                            col("Value #C", decimals=0, thresholds=steps("green", (1, "red"))),
                            col("Value #D", decimals=0, thresholds=steps("green", (1, "red"))),
                            col("Value #E", unit="celsius", decimals=0, thresholds=steps("text", (60, "orange"), (70, "red"))),
                            col("Value #F", unit="celsius", decimals=0, thresholds=steps("text", (50, "orange"), (60, "red"))),
                            col("Value #G", mappings=value_map({1: ("Available", "orange"), 2: ("Up to date", "green")})),
                            col("Value #H", unit="percent", decimals=0, thresholds=steps("text", (80, "orange"), (95, "red"))),
                            col("Value #I", unit="percent", decimals=0, thresholds=steps("text", (85, "orange"), (95, "red"))),
                            col("Value #J", unit="dtdurations", decimals=0)])


def nas_row(y, sel, title):
    return [row(title, y),
            nas_table("Synology NAS", sel, g(0, y + 1, 24, 6),
                      description="Volumes not normal: degraded, crashed, or busy (sync, scrub, expand). "
                                  "Disks not normal: status or health other than normal. Update: DSM update waiting.")]


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


# --- AIOS Experiments (Overview, collapsed) -------------------------------------------------
#
# An experimental presentation layer for the owner's AIOS task registry (Markdown). Grafana is
# NOT the task system: nothing is typed into these panels. Today they are placeholders; later
# each one shows one list from a JSON feed generated from the registry. The feed format and
# the steps to connect it: docs/aios_grafana_experiments.md.
#
# To remove the whole section: set AIOS_EXPERIMENTS = False and rerun this script. The rest of
# the Overview is unaffected either way (the row is last and starts collapsed).
AIOS_EXPERIMENTS = True
AIOS_FEED_SCHEMA = "aios-feed/1"

# (feed key, panel title, what it will list). The feed key is the contract: the generated JSON
# has the list under sections.<key>. Keep keys stable; titles can change freely.
AIOS_SECTIONS = (
    ("priorities", "Today's priorities", "What to work on today, in order."),
    ("active_projects", "Active projects", "Projects under way, with their current status."),
    ("waiting", "Blocked or waiting tasks", "Tasks that can't move, and what they wait for."),
    ("decisions", "Decisions requiring attention", "Open questions only the owner can settle."),
    ("completed", "Recently completed work", "What was finished lately, newest first."),
    ("docs_followups", "Documentation and BookStack follow-ups", "Docs and BookStack pages to write or update."),
)
# Every item in every section has these fields (only "title" is required), so each panel can
# become the same kind of table.
AIOS_ITEM_FIELDS = ("title", "project", "status", "owner", "due", "updated", "link", "note")


def aios_placeholder(key, title, what, pos):
    columns = " · ".join(f"`{f}`" for f in AIOS_ITEM_FIELDS)
    content = (
        f"**Experimental placeholder.** {what}\n\n"
        f"Not connected yet, and not the place to keep tasks: the AIOS task registry is the source "
        f"of truth.\n\n"
        f"- Feed key: `sections.{key}` (schema `{AIOS_FEED_SCHEMA}`)\n"
        f"- Columns: {columns}\n\n"
        f"How to connect: `docs/aios_grafana_experiments.md`."
    )
    panel = text(f"{title} (placeholder)", content, pos)
    panel["description"] = f"AIOS Experiments, feed key sections.{key}. Placeholder; see docs/aios_grafana_experiments.md."
    # Machine-readable marker so a later script can find and swap these panels.
    panel["options"]["aiosFeedKey"] = key
    return panel


def aios_experiments_row(y):
    """A collapsed row (click to open) holding one placeholder per AIOS section, two rows of
    three. Collapsed rows keep their panels inside the row object, so nothing below or above
    moves and the row costs nothing until it's opened."""
    r = row("AIOS Experiments (placeholders, not the task system)", y, collapsed=True)
    for i, (key, title, what) in enumerate(AIOS_SECTIONS):
        r["panels"].append(aios_placeholder(key, title, what, g((i % 3) * 8, y + 1 + (i // 3) * 8, 8, 8)))
    return r


# --- Visitors -------------------------------------------------------------------------------


def daily_barchart(title, app, pos, days=30):
    """Visitors and page views per UTC day, from the day-labelled site_daily_total series."""
    q = lambda kind: f'max by (day) (site_daily_total{{app="{app}",kind="{kind}"}})'
    return {
        "type": "barchart", "title": title, "gridPos": pos, "datasource": DS,
        "description": f"Last {days} days, UTC. Visitors = people (one per day); page views = pages they loaded.",
        "targets": [target(q("visitors"), ref="A", instant=True, table=True),
                    target(q("pageviews"), ref="B", instant=True, table=True)],
        "transformations": [
            {"id": "merge", "options": {}},
            {"id": "organize", "options": {
                "excludeByName": {"Time": True},
                "renameByName": {"day": "Day", "Value #A": "Visitors", "Value #B": "Page views"},
                "indexByName": {"day": 0, "Value #A": 1, "Value #B": 2}}},
            {"id": "sortBy", "options": {"sort": [{"field": "Day", "desc": False}]}},
        ],
        "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "decimals": 0,
                                     "custom": {"lineWidth": 0, "fillOpacity": 85}},
                        "overrides": []},
        "options": {"xField": "Day", "orientation": "vertical", "groupWidth": 0.7, "barWidth": 0.9,
                    "showValue": "never", "stacking": "none", "xTickLabelRotation": -45,
                    "xTickLabelSpacing": 100,
                    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "none"}},
    }


def breakdown_table(title, app, kind, heading, pos, value_heading, description=""):
    return table(title,
                 [target(f'site_week_breakdown{{app="{app}",kind="{kind}"}}', ref="A", instant=True, table=True)],
                 pos, {"key": heading, "Value": value_heading}, description=description,
                 overrides=[col("Value", decimals=0)], sort=(value_heading, True), cell_color=False)


def visitors():
    p = []
    explain = ("**How visitors are counted** (no cookies, nothing sent anywhere): a page view is a full "
               "page loaded by a person, not an API call, bot, link preview or probe. A visitor is one "
               "person per UTC day, recognised by an anonymous fingerprint that is thrown away at "
               "midnight UTC. People sharing a router and browser count as one. Details: "
               "`scripts/dashboards/visitors.py`.")
    for i, (app, label) in enumerate((("market", "market.poke6s.com"),
                                      ("storefront", "New storefront (not public yet: LAN visits only)"))):
        y = i * 29
        p.append(row(label, y))
        v = lambda kind, period=None: (f'sum(site_today_total{{app="{app}",kind="{kind}"}})' if period is None
                                       else f'sum(site_period_total{{app="{app}",kind="{kind}",period="{period}"}})')
        tiles = [
            ("Visitors today", v("visitors")),
            ("Page views today", v("pageviews")),
            ("Visitors yesterday", v("visitors", "yesterday")),
            ("Visits, last 7 days", v("visitors", "7d")),
            ("Visits, last 30 days", v("visitors", "30d")),
            ("Page views, last 7 days", v("pageviews", "7d")),
            ("Pages per visitor (7 days)", f'{v("pageviews", "7d")} / clamp_min({v("visitors", "7d")}, 1)'),
            ("Bot page loads today", f'sum(site_today_total{{app="{app}",kind="bot_pageviews"}})'),
        ]
        for j, (title, expr) in enumerate(tiles):
            kw = dict(decimals=1 if "per visitor" in title else 0)
            if title.startswith("Bot"):
                kw["description"] = "Crawlers, link previews and monitoring probes; not counted as visitors."
            if "last" in title and title.startswith("Visits"):
                kw["description"] = "Daily visitors added up: someone who came on two days counts twice."
            p.append(stat(title, expr, g(j * 3, y + 1, 3, 4), **kw))
        p.append(daily_barchart("Visitors and page views per day", app, g(0, y + 5, 16, 10)))
        p.append({
            "type": "bargauge", "title": "Devices (new visitors, 7 days)", "gridPos": g(16, y + 5, 8, 10),
            "datasource": DS,
            "targets": [target(f'site_week_breakdown{{app="{app}",kind="device"}}', "{{key}}", instant=True)],
            "fieldConfig": {"defaults": {"color": {"mode": "fixed", "fixedColor": "blue"}, "min": 0, "decimals": 0},
                            "overrides": []},
            "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                        "valueMode": "text", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
        })
        p.append(breakdown_table("Top pages (7 days)", app, "page", "Page", g(0, y + 15, 12, 10), "Page views"))
        p.append(breakdown_table("Where visitors came from (7 days)", app, "referrer", "From",
                                 g(12, y + 15, 12, 10), "Visitors",
                                 description="The site a new visitor arrived from. (direct) = typed the address, "
                                             "a bookmark, an app that hides the referrer, or one of our own sites."))
        if i == 0:
            p.append(text("", explain, g(0, y + 25, 24, 4)))
    return dashboard("momentum-visitors", "Pokemon Momentum: Visitors", p, refresh="5m",
                     time_from="now-30d", tags=("visitors",))


# Slowheim (the Valheim machine, 192.168.1.187) runs its own Prometheus and Loki; Grafana
# reaches them through the data sources "Slowheim Prometheus" and "Slowheim Loki"
# (grafana/provisioning/datasources.yml). Its dashboard is the owner's own export, kept as-is in
# grafana/imported/slowheim_source.json (from /home/adam/slowheim-monitoring/grafana-dashboard.json
# on Slowheim): to update it, copy that file over and rerun this script.
# --- Store (new storefront) ------------------------------------------------------------------
#
# From the storefront section of scripts/monitoring/collect_status.py, which reads
# data/storefront/store.db read-only. Orders carry mode (test or live); the "Orders" picker at
# the top defaults to live, so test orders made while building the shop never look like sales.

STORE_MODE = '"$mode"'


def store_daily_barchart(title, metrics, pos, unit="none", decimals=0, description=""):
    """Bars per local day from day-labelled series. metrics: (metric name, legend) pairs."""
    refs = "ABCDEF"
    names = {"day": "Day", **{f"Value #{refs[i]}": legend for i, (_, legend) in enumerate(metrics)}}
    return {
        "type": "barchart", "title": title, "gridPos": pos, "datasource": DS, "description": description,
        "targets": [target(f"max by (day) ({name}{{mode={STORE_MODE}}})", ref=refs[i], instant=True, table=True)
                    for i, (name, _) in enumerate(metrics)],
        "transformations": [
            {"id": "merge", "options": {}},
            {"id": "organize", "options": {"excludeByName": {"Time": True}, "renameByName": names,
                                           "indexByName": {k: i for i, k in enumerate(names)}}},
            {"id": "sortBy", "options": {"sort": [{"field": "Day", "desc": False}]}},
        ],
        "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}, "decimals": decimals, "unit": unit,
                                     "custom": {"lineWidth": 0, "fillOpacity": 85}},
                        "overrides": []},
        "options": {"xField": "Day", "orientation": "vertical", "groupWidth": 0.7, "barWidth": 0.9,
                    "showValue": "never", "stacking": "none", "xTickLabelRotation": -45,
                    "xTickLabelSpacing": 100,
                    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": len(metrics) > 1},
                    "tooltip": {"mode": "multi", "sort": "none"}},
    }


def store():
    p = [row("Orders and sales (orders picked at the top: live or test)", 0)]
    win = lambda name, w: f'sum({name}{{mode={STORE_MODE},window="{w}"}}) or vector(0)'
    tiles = [
        ("Store up (local)", 'probe_success{instance="http://localhost:8010/healthz"}',
         dict(mappings=UP_DOWN, thresholds=steps("red", (1, "green")),
              description="The storefront container answering on this host.")),
        ("Orders to ship", f"sum(momentum_store_orders_to_ship{{mode={STORE_MODE}}}) or vector(0)",
         dict(thresholds=steps("green", (1, "orange")), description="Paid orders not yet marked shipped.")),
        ("Oldest unshipped", f"max(momentum_store_oldest_unshipped_seconds{{mode={STORE_MODE}}})",
         dict(unit="dtdurations", decimals=0, thresholds=steps("green", (DAY, "orange"), (3 * DAY, "red")),
              description="Time since the oldest unshipped order was paid. No value: nothing to ship.")),
        ("Sales today", win("momentum_store_sales_dollars", "today"), dict(unit="currencyUSD", decimals=2)),
        ("Sales, 7 days", win("momentum_store_sales_dollars", "7d"), dict(unit="currencyUSD", decimals=0)),
        ("Sales, 30 days", win("momentum_store_sales_dollars", "30d"), dict(unit="currencyUSD", decimals=0,
         description="Net of refunds, by the day each order was paid (shop's local time).")),
        ("Orders, 30 days", win("momentum_store_paid_orders", "30d"), dict(decimals=0)),
        ("Average order, 30 days",
         f'sum(momentum_store_sales_dollars{{mode={STORE_MODE},window="30d"}}) / '
         f'sum(momentum_store_paid_orders{{mode={STORE_MODE},window="30d"}} > 0)',
         dict(unit="currencyUSD", decimals=2, description="No value until something sells.")),
    ]
    p += [stat(t, e, g(i * 3, 1, 3, 4), **kw) for i, (t, e, kw) in enumerate(tiles)]
    p.append(store_daily_barchart("Sales per day", [("momentum_store_daily_sales_dollars", "Sales")],
                                  g(0, 5, 10, 8), unit="currencyUSD",
                                  description="Last 30 local days, net of refunds."))
    p.append(store_daily_barchart("Orders and items per day",
                                  [("momentum_store_daily_orders", "Orders"), ("momentum_store_daily_units", "Items")],
                                  g(10, 5, 9, 8)))
    p.append(table("Orders by status",
                   [target(f"sum by (status) (momentum_store_orders{{mode={STORE_MODE}}})", ref="A",
                           instant=True, table=True)],
                   g(19, 5, 5, 8), {"status": "Status", "Value": "Orders"},
                   description="Every order ever. pending: checkout open; expired or cancelled: never paid.",
                   cell_color=False, sort=("Orders", True)))

    p.append(row("Products and stock", 13))
    vis = 'state="visible"'
    tiles = [
        ("Products on sale", f"sum(momentum_store_products{{{vis}}}) or vector(0)",
         dict(description="Visible products, stock-tracked or not.")),
        ("In stock", f"sum(momentum_store_products_in_stock{{{vis}}}) or vector(0)",
         dict(description="Visible, stock-tracked products with at least one left.")),
        ("Sold out",
         f'sum(momentum_store_products{{{vis},stock="tracked"}}) - sum(momentum_store_products_in_stock{{{vis}}})',
         dict(thresholds=steps("green", (1, "orange")),
              description="Visible, stock-tracked products with none left: customers see them as sold out.")),
        ("Low stock (1-2 left)", "count(momentum_store_low_stock > 0) or vector(0)",
         dict(thresholds=steps("green", (1, "orange")))),
        ("Units in stock", f"sum(momentum_store_stock_units{{{vis}}}) or vector(0)",
         dict(description="Visible products only.")),
        ("Stock at shop prices", f"sum(momentum_store_stock_value_dollars{{{vis}}}) or vector(0)",
         dict(unit="currencyUSD", decimals=0, description="Visible units in stock times their shop price.")),
        ("Hidden products", 'sum(momentum_store_products{state="hidden"}) or vector(0)',
         dict(description="Not shown in the shop (drafts, retired or not imported for sale).")),
        ("Hidden stock units", 'sum(momentum_store_stock_units{state="hidden"}) or vector(0)',
         dict(description="Units counted in stock on hidden products: stock nobody can buy.")),
    ]
    p += [stat(t, e, g(i * 3, 14, 3, 4), **kw) for i, (t, e, kw) in enumerate(tiles)]
    p.append(table("Nearly sold out (visible products)",
                   [target("max by (sku, title) (momentum_store_low_stock)", ref="A", instant=True, table=True)],
                   g(0, 18, 14, 10), {"title": "Product", "sku": "SKU", "Value": "Left"},
                   description="Visible, stock-tracked products with 2 or fewer left (STORE_LOW_STOCK in "
                               "collect_status.py), sold out first.",
                   overrides=[col("Value", thresholds=steps("red", (1, "orange"), (2, "text")))],
                   sort=("Left", False)))
    p.append(table("Products by kind",
                   [target("sum by (kind, state, stock) (momentum_store_products)", ref="A", instant=True, table=True),
                    target("sum by (kind, state, stock) (label_replace(momentum_store_stock_units, "
                           '"stock", "tracked", "kind", ".*"))', ref="B", instant=True, table=True)],
                   g(14, 18, 10, 10),
                   {"kind": "Kind", "state": "Shown", "stock": "Stock", "Value #A": "Products",
                    "Value #B": "Units"},
                   description="Untracked: made to order or supplier-backed, with no stock count.",
                   cell_color=False, sort=("Products", True)))
    board = dashboard("momentum-store", "Pokemon Momentum: Store", p, time_from="now-30d", tags=("store",))
    board["templating"]["list"].append({
        "type": "custom", "name": "mode", "label": "Orders", "query": "live,test",
        "current": {"text": "live", "value": "live"}, "hide": 0,
        "options": [{"text": v, "value": v, "selected": v == "live"} for v in ("live", "test")],
        "description": "live: real orders. test: orders from Stripe test mode while building the shop.",
    })
    return board


SLOWHEIM_SOURCE =Path(__file__).resolve().parent / "grafana" / "imported" / "slowheim_source.json"
SLOWHEIM_DATASOURCES = {"${DS_PROMETHEUS}": "slowheim-prometheus", "${DS_LOKI}": "slowheim-loki"}


def slowheim():
    """The imported Slowheim dashboard with its data-source prompts filled in, the Home lab tag
    and the same top bar as the others."""
    text = SLOWHEIM_SOURCE.read_text(encoding="utf-8")
    for placeholder, uid in SLOWHEIM_DATASOURCES.items():
        text = text.replace(placeholder, uid)
    board = json.loads(text)
    for key in ("__inputs", "__requires", "__elements", "id"):
        board.pop(key, None)
    frame = dashboard("homelab-slowheim", "Home lab: Slowheim (Valheim)", [], tags=("valheim",), family="homelab")
    board.update(uid=frame["uid"], title=frame["title"], tags=frame["tags"], links=frame["links"])
    return board


def main() -> None:
    boards = ((OUT, "overview", overview()), (OUT, "website", website()),
              (OUT, "jobs_and_work", jobs_and_work()), (OUT, "visitors", visitors()),
              (OUT, "store", store()),
              (OUT_HOMELAB, "proxmox", proxmox()), (OUT_HOMELAB, "network", network()),
              (OUT_HOMELAB, "servers", servers()), (OUT_HOMELAB, "slowheim", slowheim()))
    for folder, name, board in boards:
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{name}.json").write_text(json.dumps(board, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {folder / f'{name}.json'} ({len(board['panels'])} panels)")
    # Dashboards that moved folder (2026-11-05); a stale copy would clash on uid.
    for old in ("home_lab.json", "servers.json"):
        (OUT / old).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
