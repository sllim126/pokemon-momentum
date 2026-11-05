#!/usr/bin/env python3
"""Run a cron job and record how it went, for the Grafana "Did the jobs run?" panel.

Usage (in adam's crontab, see deploy/crontab.txt):

    python3 scripts/monitoring/run_job.py <job-name> -- <command> [args...]

The command runs exactly as before: same output (so the existing ``>> logs/x.log 2>&1``
redirect still catches it) and same exit code. Afterwards this writes
``momentum_job_<job-name>.prom`` into node-exporter's text-file folder with the start and
finish times, duration, exit code and the time of the last successful run. The systemd
jobs get the same metrics from scripts/monitoring/collect_status.py instead, and that file
also lists how often each job should run. Use the same <job-name> as its JOBS table.

Standard library only; runs on the host's Python. A failure to write the metrics file is
printed but never changes the job's exit code.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

TEXTFILE_DIR = Path(os.environ.get("MOMENTUM_TEXTFILE_DIR", "/var/lib/prometheus/node-exporter"))

# Help text for the job metrics, shared with collect_status.py (which imports it). Both
# scripts write these metrics into the same text-file folder, and node-exporter drops a
# metric entirely when two files give it different HELP or TYPE lines; on 2026-11-05 that
# hid the systemd jobs' results. So every file declares them identically, from here.
JOB_HELP = {
    "momentum_job_last_start_timestamp_seconds": "When the job last started",
    "momentum_job_last_run_timestamp_seconds": "When the job last finished",
    "momentum_job_last_duration_seconds": "How long the last run took",
    "momentum_job_last_exit_code": "Exit code of the last run (0 = ok)",
    "momentum_job_running": "1 while the job is running",
    "momentum_job_last_success_timestamp_seconds": "When the job last finished without an error",
}


def job_metric_lines(name: str, task: str, value: float) -> list[str]:
    return [f"# HELP {name} {JOB_HELP[name]}", f"# TYPE {name} gauge", f'{name}{{task="{task}"}} {value}']


def _previous_success(path: Path, job: str) -> float | None:
    """Carry the last success time over from the previous file when this run failed."""
    try:
        text = path.read_text()
    except OSError:
        return None
    match = re.search(rf'^momentum_job_last_success_timestamp_seconds{{task="{re.escape(job)}"}} (\S+)$', text, re.M)
    return float(match.group(1)) if match else None


def _write(job: str, started: float, finished: float, code: int) -> None:
    path = TEXTFILE_DIR / f"momentum_job_{job}.prom"
    success = finished if code == 0 else _previous_success(path, job)
    values = {
        "momentum_job_last_start_timestamp_seconds": started,
        "momentum_job_last_run_timestamp_seconds": finished,
        "momentum_job_last_duration_seconds": finished - started,
        "momentum_job_last_exit_code": code,
        "momentum_job_running": 0,
        "momentum_job_last_success_timestamp_seconds": success,
    }
    lines = [line for name, value in values.items() if value is not None
             for line in job_metric_lines(name, job, value)]
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text("\n".join(lines) + "\n")
    tmp.chmod(0o644)
    tmp.replace(path)


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[1] != "--" or not re.fullmatch(r"[a-z0-9-]+", argv[0]):
        print("usage: run_job.py <job-name> -- <command> [args...]", file=sys.stderr)
        return 2
    job, command = argv[0], argv[2:]
    started = time.time()
    try:
        code = subprocess.run(command).returncode
    except OSError as exc:  # command not found and the like
        print(f"run_job: could not start {command[0]}: {exc}", file=sys.stderr)
        code = 127
    try:
        _write(job, started, time.time(), code)
    except OSError as exc:
        print(f"run_job: could not write metrics for {job}: {exc}", file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
