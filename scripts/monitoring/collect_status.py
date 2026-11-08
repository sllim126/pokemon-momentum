#!/usr/bin/env python3
"""Write the project's status as Prometheus metrics for the Grafana overview dashboard.

Runs on the host (not in Docker) every minute from adam's crontab and writes
``momentum_status.prom`` into node-exporter's text-file folder, which Prometheus scrapes.
Standard library only, so it runs on the host's own Python. See "Monitoring" in
docs/operator_runbook.txt for the whole setup.

What it reports (all metric names start with ``momentum_``):

* systemd jobs: when each pokemon-momentum-* unit last started and finished, its exit
  code, when it last succeeded and when its timer fires next. Cron jobs report the same
  ``momentum_job_*`` metrics themselves, through scripts/monitoring/run_job.py.
* ``momentum_job_expected_interval_seconds``: how often every job (systemd or cron) should
  run, so Grafana can show "overdue" without knowing the schedule. Keep JOBS below in step
  with the schedule in docs/operator_runbook.txt.
* Data downloads: the newest PriceCharting CSV, TCGCSV archive day, parquet history,
  catalogue crawl and storefront price file.
* Log files: when each file in logs/ was last written.
* docs/todo.txt: items per status tag and the "Last updated" date.
* Bug reports from Discord (tracking.sqlite3): reports per status, newest report time.
* Git: current branch, uncommitted project files, commits not yet pushed.
* A reminder while the temporary sudo file from the monitoring build-out still exists.
* Plex (home lab, since 2026-11-06): streams now, how many transcode, and items per library,
  from the Plex server's own API. Needs the owner's Plex token in PLEX_TOKEN_FILE (mode 600,
  outside the repo); without that file the section publishes nothing.
* Proxmox Backup Server (home lab, since 2026-11-08): datastore space, each backed-up guest's
  newest backup, snapshot count and verify result, and the outcome of backup, verify, prune
  and garbage-collection tasks. Needs a read-only (Audit) API token in PBS_TOKEN_FILE; TLS is
  verified against PBS's own certificate (PBS_CERT_FILE). Without the token, nothing.

Every section is fenced off with its own try/except: one failing check (say, a locked
database) is reported as ``momentum_collector_errors{section=...}`` and the rest still
publishes. The file is written to a temporary name and renamed, so Prometheus never reads
a half-written file.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_job import JOB_HELP  # noqa: E402  (same help text in both scripts; see run_job.py)
TEXTFILE_DIR = Path(os.environ.get("MOMENTUM_TEXTFILE_DIR", "/var/lib/prometheus/node-exporter"))
OUTPUT = TEXTFILE_DIR / "momentum_status.prom"
# Remembers when each systemd job last succeeded, because systemd only keeps the latest run.
STATE_FILE = TEXTFILE_DIR / ".momentum_status_state.json"
SUDOERS_REMINDER = Path("/etc/sudoers.d/90-claude-buildout")

# Plex: the token gives full access to the owner's Plex account, so it lives only in this
# root-made, adam-only file and is never logged or put in a metric.
PLEX_URL = os.environ.get("MOMENTUM_PLEX_URL", "http://192.168.1.219:32400")
PLEX_TOKEN_FILE = Path(os.environ.get("MOMENTUM_PLEX_TOKEN_FILE", "/etc/momentum-monitoring/plex-token"))

# Proxmox Backup Server. The token file holds "user@realm!tokenname:secret" (an API token
# with the Audit role only); PBS_CERT_FILE is PBS's self-signed certificate, saved from
# the server once (runbook "Proxmox Backup Server"). The certificate names the server
# "72pbs", not its IP, so the TLS check uses that name.
PBS_HOST = os.environ.get("MOMENTUM_PBS_HOST", "192.168.1.72")
PBS_CERT_NAME = os.environ.get("MOMENTUM_PBS_CERT_NAME", "72pbs")
PBS_TOKEN_FILE = Path(os.environ.get("MOMENTUM_PBS_TOKEN_FILE", "/etc/momentum-monitoring/pbs-token"))
PBS_CERT_FILE = Path(os.environ.get("MOMENTUM_PBS_CERT_FILE", "/etc/momentum-monitoring/pbs-cert.pem"))
PBS_TASK_TYPES = ("backup", "verify", "verificationjob", "prune", "prunejob", "garbage_collection", "syncjob")

DAY = 86400
# job name -> (how it runs, expected interval in seconds). "systemd:<unit>" units are read
# here; "cron" jobs write their own metrics through run_job.py under the same name.
JOBS = {
    "daily-update": ("systemd:pokemon-momentum-daily-update", DAY),
    "tcgcsv-collect": ("systemd:pokemon-momentum-tcgcsv-collect", DAY),
    "weekly-refresh": ("systemd:pokemon-momentum-weekly-refresh", 7 * DAY),
    "weekly-placeholder-refresh": ("systemd:pokemon-momentum-weekly-placeholder-refresh", 7 * DAY),
    "storefront-market-reference": ("cron", DAY),
    "placeholder-sync": ("cron", DAY),
    "squarespace-price-sync": ("cron", DAY),
    "tcgcsv-catalogue": ("cron", DAY),
    "archive-backfill": ("cron", DAY),
    "discord-bug-import": ("cron", 15 * 60),
    "discord-weekly": ("cron", 7 * DAY),
}

TODO_TAGS = ("next", "in progress", "waiting", "planned", "parked", "idea", "watching")


class Metrics:
    """Collects lines in the Prometheus text format, one HELP/TYPE header per metric."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.declared: set[str] = set()

    def add(self, name: str, value: float | int | None, help_text: str, **labels: str) -> None:
        if value is None:
            return
        if name not in self.declared:
            self.lines += [f"# HELP {name} {help_text}", f"# TYPE {name} gauge"]
            self.declared.add(name)
        label_text = ",".join(f'{k}="{_escape(str(v))}"' for k, v in labels.items())
        self.lines.append(f"{name}{{{label_text}}} {float(value)}" if labels else f"{name} {float(value)}")

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _newest_mtime(path: Path, pattern: str = "*") -> float | None:
    if path.is_file():
        return path.stat().st_mtime
    if not path.is_dir():
        return None
    times = [p.stat().st_mtime for p in path.glob(pattern) if p.is_file()]
    return max(times) if times else None


# --- sections ------------------------------------------------------------------------------


def systemd_jobs(m: Metrics, state: dict) -> None:
    """Metrics for each systemd job in JOBS: last start and finish, exit code, last success, running
    now, and whether its timer is active and when it fires next (read with systemctl show).
    """
    for job, (kind, interval) in JOBS.items():
        m.add("momentum_job_expected_interval_seconds", interval,
              "How often the job should run", task=job, runner=kind.split(":")[0])
        if not kind.startswith("systemd:"):
            continue
        unit = kind.split(":", 1)[1]
        props = _systemctl_show(f"{unit}.service", "ExecMainStartTimestamp", "ExecMainExitTimestamp",
                                "ExecMainStatus", "Result", "ActiveState")
        timer = _systemctl_show(f"{unit}.timer", "NextElapseUSecRealtime", "LastTriggerUSec", "ActiveState")
        started = _unix(props.get("ExecMainStartTimestamp"))
        if started is None:
            # systemd forgets service runs at reboot; the timer's last trigger survives.
            started = _unix(timer.get("LastTriggerUSec"))
        finished = _unix(props.get("ExecMainExitTimestamp"))
        running = props.get("ActiveState") in {"active", "activating"}
        m.add("momentum_job_running", 1 if running else 0, JOB_HELP["momentum_job_running"], task=job)
        m.add("momentum_job_last_start_timestamp_seconds", started, JOB_HELP["momentum_job_last_start_timestamp_seconds"], task=job)
        if finished:
            code = int(props.get("ExecMainStatus") or 0)
            ok = props.get("Result") == "success" and code == 0
            m.add("momentum_job_last_run_timestamp_seconds", finished, JOB_HELP["momentum_job_last_run_timestamp_seconds"], task=job)
            m.add("momentum_job_last_exit_code", code, JOB_HELP["momentum_job_last_exit_code"], task=job)
            if started:
                m.add("momentum_job_last_duration_seconds", max(0.0, finished - started),
                      JOB_HELP["momentum_job_last_duration_seconds"], task=job)
            if ok:
                state[job] = finished
        m.add("momentum_job_last_success_timestamp_seconds", state.get(job),
              JOB_HELP["momentum_job_last_success_timestamp_seconds"], task=job)
        m.add("momentum_job_next_run_timestamp_seconds", _unix(timer.get("NextElapseUSecRealtime")),
              "When the systemd timer fires next", task=job)
        m.add("momentum_job_timer_active", 1 if timer.get("ActiveState") == "active" else 0,
              "1 when the systemd timer is switched on", task=job)


def _systemctl_show(unit: str, *props: str) -> dict:
    out = subprocess.run(
        ["systemctl", "show", "--timestamp=unix", unit, *[f"-p{p}" for p in props]],
        capture_output=True, text=True, timeout=10,
    ).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def _unix(value: str | None) -> float | None:
    # systemd prints "@1793845058" with --timestamp=unix, but some properties (the timer's
    # LastTriggerUSec) ignore that flag and print "Sun 2026-11-01 11:00:01 UTC". Empty or
    # "n/a" means it has never run.
    if not value or value == "n/a":
        return None
    if value.startswith("@"):
        return float(value[1:])
    try:
        return datetime.strptime(value, "%a %Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def data_downloads(m: Metrics) -> None:
    """When each data source last changed (PriceCharting CSV, TCGCSV archive and catalogue, parquet,
    market prices CSV, storefront market reference), so
    Grafana can show a feed that stopped arriving.
    """
    help_text = "When this data source was last downloaded or rebuilt"
    sources = {
        "pricecharting_csv": _newest_mtime(ROOT / "data/pricecharting/raw"),
        "tcgcsv_archive": _newest_mtime_dirs(ROOT / "data/tcgcsv_archive"),
        "tcgcsv_catalogue": _newest_mtime_dirs(ROOT / "data/tcgcsv_catalogue"),
        "parquet_history": _newest_mtime_dirs(ROOT / "data/parquet"),
        "market_prices_csv": _newest_mtime(ROOT / "data/market_prices_latest.csv"),
        "storefront_market_reference": _newest_mtime(ROOT / "data/storefront/market_reference.json"),
    }
    for source, mtime in sources.items():
        m.add("momentum_data_updated_timestamp_seconds", mtime, help_text, source=source)
    # The archive folder is named by price date, which can lag the download by a day.
    days = [p.name for p in (ROOT / "data/tcgcsv_archive").iterdir() if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name)]
    if days:
        newest = datetime.strptime(max(days), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        m.add("momentum_tcgcsv_archive_latest_day_timestamp_seconds", newest.timestamp(),
              "Price date of the newest TCGCSV archive folder")


def _newest_mtime_dirs(path: Path) -> float | None:
    """Newest modification time among the folder and its direct children (one level only:
    the parquet and archive trees hold tens of thousands of files)."""
    if not path.exists():
        return None
    times = [path.stat().st_mtime] + [p.stat().st_mtime for p in path.iterdir()]
    return max(times)


def logs(m: Metrics) -> None:
    for log in sorted((ROOT / "logs").glob("*.log")):
        stat = log.stat()
        m.add("momentum_log_updated_timestamp_seconds", stat.st_mtime, "When the log was last written", log=log.stem)
        m.add("momentum_log_size_bytes", stat.st_size, "Size of the log file", log=log.stem)


def todo(m: Metrics) -> None:
    """Count docs/todo.txt items per status tag ([next], [planned], ...), leaving out Recently Completed."""
    path = ROOT / "docs/todo.txt"
    text = path.read_text(encoding="utf-8")
    # Count list items ("- [next] ...", at any indent) in every section except "Recently
    # Completed". A section heading is a line underlined with dashes; the tag legend at the
    # top has no "- " before its tags, so it isn't counted.
    sections = re.split(r"^(.+)\n-{3,}\s*$", text, flags=re.M)
    open_part = "".join(
        body for heading, body in zip(sections[1::2], sections[2::2])
        if heading.strip() != "Recently Completed"
    )
    for tag in TODO_TAGS:
        count = len(re.findall(rf"^\s*- \[{re.escape(tag)}\b[^\]]*\]", open_part, re.M))
        m.add("momentum_todo_items", count, "Open items in docs/todo.txt by status tag", status=tag)
    match = re.search(r"^Last updated:\s*(\d{4}-\d{2}-\d{2})", text, re.M)
    if match:
        day = datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        m.add("momentum_todo_last_updated_timestamp_seconds", day.timestamp(), "The 'Last updated' date in docs/todo.txt")
    m.add("momentum_todo_file_modified_timestamp_seconds", path.stat().st_mtime, "When docs/todo.txt was last saved")


def bug_reports(m: Metrics) -> None:
    db = ROOT / "data/processed/tracking.sqlite3"
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
    try:
        for status, count in con.execute("SELECT status, count(*) FROM bug_reports GROUP BY status"):
            m.add("momentum_bug_reports", count, "Bug reports by status (admin page /bug-reports)", status=status)
        newest = con.execute("SELECT max(created_at) FROM bug_reports").fetchone()[0]
        if newest:
            m.add("momentum_bug_report_newest_timestamp_seconds", _iso(newest), "When the newest bug report came in")
        users = con.execute("SELECT count(*) FROM tracking_users").fetchone()[0]
        m.add("momentum_site_accounts", users, "Signed-up accounts on the market site")
    finally:
        con.close()


def _iso(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def git(m: Metrics) -> None:
    """Repository health: on main or not, uncommitted project files (data files excluded),
    commits not pushed to origin, and when main last changed.
    """
    def run(*args: str) -> str:
        return subprocess.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True, timeout=20).stdout

    branch = run("rev-parse", "--abbrev-ref", "HEAD").strip()
    m.add("momentum_git_branch_info", 1, "Branch the repo is on (label)", branch=branch)
    m.add("momentum_git_on_main", 1 if branch == "main" else 0, "1 when the repo is on main")
    # Data files are changed by the jobs every day and are never committed, so leave them out
    # (AGENTS.md: "Data files are not commits").
    changed = [
        line for line in run("status", "--porcelain").splitlines()
        if not re.search(r"(^.. \"?data/|products_.*\.csv|TCG Placeholders/)", line)
    ]
    m.add("momentum_git_uncommitted_files", len(changed), "Changed project files not yet committed (data files excluded)")
    ahead = run("rev-list", "--count", "origin/main..main").strip()
    m.add("momentum_git_unpushed_commits", int(ahead or 0), "Commits on main not yet pushed to origin")
    last = run("log", "-1", "--format=%ct", "main").strip()
    m.add("momentum_git_last_commit_timestamp_seconds", int(last or 0) or None, "Time of the newest commit on main")


def reminders(m: Metrics) -> None:
    m.add("momentum_temporary_sudo_present", 1 if SUDOERS_REMINDER.exists() else 0,
          "1 while /etc/sudoers.d/90-claude-buildout exists (remove it after the monitoring build-out)")


def _plex_get(path: str, token: str, count_only: bool = False) -> dict:
    """GET a Plex API path as JSON. count_only asks for zero rows, so Plex just returns
    totalSize (cheap even for a library of thousands of items)."""
    import urllib.request

    headers = {"X-Plex-Token": token, "Accept": "application/json"}
    if count_only:
        headers |= {"X-Plex-Container-Start": "0", "X-Plex-Container-Size": "0"}
    with urllib.request.urlopen(urllib.request.Request(PLEX_URL + path, headers=headers), timeout=10) as resp:
        return json.load(resp)["MediaContainer"]


def plex(m: Metrics) -> None:
    """Streams and library sizes on the home lab Plex server (Home lab: Servers dashboard).
    Library names come from Plex itself; show libraries also report their episode count."""
    if not PLEX_TOKEN_FILE.exists():
        return
    token = PLEX_TOKEN_FILE.read_text().strip()
    sessions = _plex_get("/status/sessions", token).get("Metadata", [])
    states: dict[str, int] = {"playing": 0, "paused": 0, "buffering": 0}
    for item in sessions:
        state = item.get("Player", {}).get("state", "playing")
        states[state] = states.get(state, 0) + 1
    for state, count in states.items():
        m.add("momentum_plex_streams", count, "Plex streams now, by player state", state=state)
    m.add("momentum_plex_transcodes", sum(1 for item in sessions if "TranscodeSession" in item),
          "Plex streams being transcoded now (the rest play directly)")
    for lib in _plex_get("/library/sections", token).get("Directory", []):
        key, kind, title = lib["key"], lib.get("type", ""), lib.get("title", "")
        items = _plex_get(f"/library/sections/{key}/all", token, count_only=True).get("totalSize")
        m.add("momentum_plex_library_items", items,
              "Items in each Plex library (movies, shows, artists or photos)", library=title, type=kind)
        if kind == "show":  # type=4 counts episodes rather than shows
            episodes = _plex_get(f"/library/sections/{key}/all?type=4", token, count_only=True).get("totalSize")
            m.add("momentum_plex_library_episodes", episodes, "Episodes in each Plex TV library", library=title)
    m.add("momentum_plex_up", 1, "1 when collect_status.py could read the Plex API")


def _pbs_get(path: str, token: str) -> list | dict:
    """GET /api2/json<path> from PBS and return its "data". Connects to PBS_HOST but checks
    the certificate (pinned file, hostname PBS_CERT_NAME) as normal TLS would."""
    import http.client
    import socket
    import ssl

    ctx = ssl.create_default_context(cafile=str(PBS_CERT_FILE))

    class Conn(http.client.HTTPSConnection):
        def connect(self) -> None:
            sock = socket.create_connection((self.host, self.port), self.timeout)
            self.sock = ctx.wrap_socket(sock, server_hostname=PBS_CERT_NAME)

    conn = Conn(PBS_HOST, 8007, timeout=15)
    try:
        conn.request("GET", "/api2/json" + path, headers={"Authorization": f"PBSAPIToken={token}"})
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise RuntimeError(f"PBS {path}: HTTP {resp.status}")  # never include the token
        return json.loads(body)["data"]
    finally:
        conn.close()


def _pbs_task_ok(task: dict) -> bool:
    """PBS task status is "OK", "WARNINGS: <n>" (still a success) or the error message."""
    return str(task.get("status", "")).startswith(("OK", "WARNINGS"))


def pbs(m: Metrics, now: float | None = None) -> None:
    """Backups on the Proxmox Backup Server (Home lab: Proxmox cluster dashboard)."""
    if not PBS_TOKEN_FILE.exists():
        return
    token = PBS_TOKEN_FILE.read_text().strip()
    now = now or time.time()
    for ds in _pbs_get("/status/datastore-usage", token):
        store = ds["store"]
        for kind in ("total", "used", "avail"):
            m.add("momentum_pbs_datastore_bytes", ds.get(kind), "PBS datastore space (kind: total, used, avail)",
                  store=store, kind=kind)
        # The newest snapshot of each group carries the verify result we care about.
        newest: dict[tuple, dict] = {}
        for snap in _pbs_get(f"/admin/datastore/{store}/snapshots", token):
            key = (snap["backup-type"], snap["backup-id"])
            if key not in newest or snap["backup-time"] > newest[key]["backup-time"]:
                newest[key] = snap
        for group in _pbs_get(f"/admin/datastore/{store}/groups", token):
            key = (group["backup-type"], group["backup-id"])
            labels = dict(store=store, type=key[0], id=key[1], guest=(group.get("comment") or "").strip())
            m.add("momentum_pbs_group_last_backup_timestamp_seconds", group.get("last-backup"),
                  "Newest backup of each guest (backup group) on PBS", **labels)
            m.add("momentum_pbs_group_snapshots", group.get("backup-count"),
                  "Backups kept for each guest on PBS", **labels)
            state = (newest.get(key, {}).get("verification") or {}).get("state")
            m.add("momentum_pbs_group_last_verify", {"ok": 1, "failed": 0}.get(state, 2),
                  "Verify result of each guest's newest backup: 1 ok, 0 failed, 2 not verified yet", **labels)
    # Task history for the last 7 days. Running tasks have no endtime yet and are skipped.
    tasks = _pbs_get(f"/nodes/localhost/tasks?since={int(now - 7 * DAY)}&limit=5000", token)
    for kind in PBS_TASK_TYPES:
        done = sorted((t for t in tasks if t.get("worker_type") == kind and t.get("endtime")),
                      key=lambda t: t["endtime"])
        failures = sum(1 for t in done if not _pbs_task_ok(t))
        m.add("momentum_pbs_task_failures_7d", failures, "PBS tasks that failed in the last 7 days, by type",
              type=kind)
        if done:
            m.add("momentum_pbs_task_last_end_timestamp_seconds", done[-1]["endtime"],
                  "When the newest finished PBS task of each type ended", type=kind)
            m.add("momentum_pbs_task_last_ok", 1 if _pbs_task_ok(done[-1]) else 0,
                  "1 when the newest finished PBS task of each type succeeded", type=kind)
    m.add("momentum_pbs_up", 1, "1 when collect_status.py could read the PBS API")


# --- main ----------------------------------------------------------------------------------


def main() -> int:
    """Collect every section into one metrics file. A section that fails is reported (as a metric and
    on stderr) without stopping the others; both output files are written atomically.
    """
    m = Metrics()
    try:
        state = json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        state = {}
    sections = {
        "systemd_jobs": lambda: systemd_jobs(m, state),
        "data_downloads": lambda: data_downloads(m),
        "logs": lambda: logs(m),
        "todo": lambda: todo(m),
        "bug_reports": lambda: bug_reports(m),
        "git": lambda: git(m),
        "reminders": lambda: reminders(m),
        "plex": lambda: plex(m),
        "pbs": lambda: pbs(m),
    }
    errors = {}
    for name, section in sections.items():
        try:
            section()
            errors[name] = 0
        except Exception as exc:  # report and carry on; see module docstring
            errors[name] = 1
            print(f"collect_status: {name} failed: {exc!r}", file=sys.stderr)
    for name, failed in errors.items():
        m.add("momentum_collector_errors", failed, "1 when this section of collect_status.py failed", section=name)
    m.add("momentum_collector_last_run_timestamp_seconds", time.time(), "When collect_status.py last ran")

    _write_atomic(OUTPUT, m.text())
    _write_atomic(STATE_FILE, json.dumps(state))
    return 0


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.chmod(0o644)
    tmp.replace(path)


if __name__ == "__main__":
    sys.exit(main())
