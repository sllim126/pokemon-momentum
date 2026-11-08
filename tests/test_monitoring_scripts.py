"""Host-side monitoring scripts (scripts/monitoring/, deploy/monitoring/zfs/): todo counts,
job status files, Plex, PBS and ZFS pool parsing."""

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

MONITORING = Path(__file__).resolve().parents[1] / "scripts" / "monitoring"


def _load(name):
    spec = importlib.util.spec_from_file_location(name, MONITORING / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_from(folder, name):
    path = Path(__file__).resolve().parents[1] / folder / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collect_status = _load("collect_status")
run_job = _load("run_job")


class TodoCountTests(unittest.TestCase):
    def test_counts_open_items_but_not_the_legend_or_recently_completed(self):
        text = (
            "Status tags used below:\n\n  [next]  pick these up first\n\n"
            "Recently Completed\n------------------\n\n- [next] finished already\n\n"
            "Website\n-------\n\n- [next] one\n  - [next] nested two\n- [waiting] three\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "docs").mkdir()
            (root / "docs" / "todo.txt").write_text("Last updated: 2026-11-05\n\n" + text)
            original, collect_status.ROOT = collect_status.ROOT, root
            try:
                metrics = collect_status.Metrics()
                collect_status.todo(metrics)
            finally:
                collect_status.ROOT = original
        out = metrics.text()
        self.assertIn('momentum_todo_items{status="next"} 2.0', out)
        self.assertIn('momentum_todo_items{status="waiting"} 1.0', out)


class PlexTests(unittest.TestCase):
    def test_counts_streams_transcodes_and_libraries_without_a_real_server(self):
        replies = {
            "/status/sessions": {"Metadata": [
                {"Player": {"state": "playing"}, "TranscodeSession": {}},
                {"Player": {"state": "paused"}},
            ]},
            "/library/sections": {"Directory": [{"key": "1", "type": "movie", "title": "Movies"},
                                                {"key": "2", "type": "show", "title": "TV Shows"}]},
            "/library/sections/1/all": {"totalSize": 10},
            "/library/sections/2/all": {"totalSize": 3},
            "/library/sections/2/all?type=4": {"totalSize": 40},
        }
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "plex-token"
            token.write_text("not-a-real-token\n")
            saved = collect_status.PLEX_TOKEN_FILE, collect_status._plex_get
            collect_status.PLEX_TOKEN_FILE = token
            collect_status._plex_get = lambda path, tok, count_only=False: replies[path]
            try:
                metrics = collect_status.Metrics()
                collect_status.plex(metrics)
            finally:
                collect_status.PLEX_TOKEN_FILE, collect_status._plex_get = saved
        out = metrics.text()
        self.assertIn('momentum_plex_streams{state="playing"} 1.0', out)
        self.assertIn('momentum_plex_streams{state="paused"} 1.0', out)
        self.assertIn("momentum_plex_transcodes 1.0", out)
        self.assertIn('momentum_plex_library_items{library="Movies",type="movie"} 10.0', out)
        self.assertIn('momentum_plex_library_episodes{library="TV Shows"} 40.0', out)
        self.assertNotIn("not-a-real-token", out)

    def test_publishes_nothing_without_a_token_file(self):
        saved = collect_status.PLEX_TOKEN_FILE
        collect_status.PLEX_TOKEN_FILE = Path("/nonexistent/plex-token")
        try:
            metrics = collect_status.Metrics()
            collect_status.plex(metrics)
        finally:
            collect_status.PLEX_TOKEN_FILE = saved
        self.assertEqual(metrics.lines, [])


class PbsTests(unittest.TestCase):
    NOW = 1_800_000_000

    def _run(self, replies):
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "pbs-token"
            token.write_text("monitor@pbs!grafana:not-a-real-secret\n")
            saved = collect_status.PBS_TOKEN_FILE, collect_status._pbs_get
            collect_status.PBS_TOKEN_FILE = token
            collect_status._pbs_get = lambda path, tok: replies[path.split("?")[0]]
            try:
                metrics = collect_status.Metrics()
                collect_status.pbs(metrics, now=self.NOW)
            finally:
                collect_status.PBS_TOKEN_FILE, collect_status._pbs_get = saved
        return metrics.text()

    def test_backups_verify_state_and_task_results(self):
        out = self._run({
            "/status/datastore-usage": [{"store": "main", "total": 100, "used": 40, "avail": 60}],
            "/admin/datastore/main/snapshots": [
                {"backup-type": "vm", "backup-id": "101", "backup-time": 1, "verification": {"state": "failed"}},
                {"backup-type": "vm", "backup-id": "101", "backup-time": 2, "verification": {"state": "ok"}},
                {"backup-type": "ct", "backup-id": "105", "backup-time": 3},
            ],
            "/admin/datastore/main/groups": [
                {"backup-type": "vm", "backup-id": "101", "last-backup": 2, "backup-count": 2, "comment": "grafana"},
                {"backup-type": "ct", "backup-id": "105", "last-backup": 3, "backup-count": 1},
            ],
            "/nodes/localhost/tasks": [
                {"worker_type": "backup", "endtime": 10, "status": "OK"},
                {"worker_type": "backup", "endtime": 20, "status": "backup failed: connection lost"},
                {"worker_type": "verify", "endtime": 15, "status": "WARNINGS: 1"},
                {"worker_type": "prune", "starttime": 30},  # still running
            ],
        })
        self.assertIn('momentum_pbs_datastore_bytes{store="main",kind="used"} 40.0', out)
        # The newest snapshot's verify result counts, not an older failed one.
        self.assertIn('momentum_pbs_group_last_verify{store="main",type="vm",id="101",guest="grafana"} 1.0', out)
        self.assertIn('momentum_pbs_group_last_verify{store="main",type="ct",id="105",guest=""} 2.0', out)
        self.assertIn('momentum_pbs_task_failures_7d{type="backup"} 1.0', out)
        self.assertIn('momentum_pbs_task_last_ok{type="backup"} 0.0', out)
        self.assertIn('momentum_pbs_task_last_ok{type="verify"} 1.0', out)
        self.assertNotIn('momentum_pbs_task_last_ok{type="prune"}', out)
        self.assertNotIn("not-a-real-secret", out)


ZPOOL_STATUS = """  pool: zfs
 state: ONLINE
  scan: scrub repaired 0 in 05:12:33 with 2 errors on Sun Nov  9 05:36:34 2025
config:

	NAME                        STATE     READ WRITE CKSUM
	zfs                         ONLINE       0     0     0
	  raidz2-0                  ONLINE       0     0     0
	    ata-DISK_A              ONLINE       0     0     3
	    ata-DISK_B              FAULTED      1     0     0  too many errors

errors: 4 data errors, use '-v' for a list
"""


class ZfsStatusTests(unittest.TestCase):
    zfs_status = _load_from("deploy/monitoring/zfs", "zfs_status")

    def test_parses_scrub_devices_and_data_errors(self):
        status = self.zfs_status.parse_status(ZPOOL_STATUS)
        self.assertEqual(status["scrub_errors"], 2)
        self.assertIsNotNone(status["scrub_end"])
        self.assertIsNone(status["in_progress"])
        self.assertEqual(status["data_errors"], 4)
        names = [d["device"] for d in status["devices"]]
        self.assertEqual(names, ["zfs", "raidz2-0", "ata-DISK_A", "ata-DISK_B"])
        disk_b = status["devices"][3]
        self.assertEqual((disk_b["state"], disk_b["read"]), ("FAULTED", 1))
        self.assertEqual(status["devices"][2]["checksum"], 3)

    def test_scrub_in_progress_and_clean_pool(self):
        text = ZPOOL_STATUS.replace(
            "scan: scrub repaired 0 in 05:12:33 with 2 errors on Sun Nov  9 05:36:34 2025",
            "scan: scrub in progress since Sun Nov  9 00:24:01 2025",
        ).replace("errors: 4 data errors, use '-v' for a list", "errors: No known data errors")
        status = self.zfs_status.parse_status(text)
        self.assertEqual(status["in_progress"], "scrub")
        self.assertIsNone(status["scrub_end"])
        self.assertEqual(status["data_errors"], 0)


class RunJobTests(unittest.TestCase):
    def test_records_exit_code_and_keeps_last_success_after_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_job.TEXTFILE_DIR = Path(tmp)
            self.assertEqual(run_job.main(["demo", "--", sys.executable, "-c", "pass"]), 0)
            first = (Path(tmp) / "momentum_job_demo.prom").read_text()
            success_line = next(l for l in first.splitlines() if "last_success" in l)

            self.assertEqual(run_job.main(["demo", "--", sys.executable, "-c", "raise SystemExit(3)"]), 3)
            second = (Path(tmp) / "momentum_job_demo.prom").read_text()
        self.assertIn('momentum_job_last_exit_code{task="demo"} 3', second)
        self.assertIn(success_line, second)

    def test_job_files_and_collector_declare_the_same_help(self):
        # node-exporter drops a metric when two text files give it different HELP/TYPE
        # lines; that hid the systemd jobs' results on 2026-11-05.
        def help_lines(text):
            return {l.split()[2]: l for l in text.splitlines() if l.startswith(("# HELP momentum_job_", "# TYPE momentum_job_"))
                    and l.split()[2] in run_job.JOB_HELP and l.startswith("# HELP")}

        with tempfile.TemporaryDirectory() as tmp:
            run_job.TEXTFILE_DIR = Path(tmp)
            run_job.main(["demo", "--", sys.executable, "-c", "pass"])
            job_file = help_lines((Path(tmp) / "momentum_job_demo.prom").read_text())
        metrics = collect_status.Metrics()
        for name, text in collect_status.JOB_HELP.items():
            metrics.add(name, 1, text, task="x")
        self.assertEqual(job_file, help_lines(metrics.text()))
        self.assertEqual(len(job_file), len(run_job.JOB_HELP))

    def test_rejects_bad_usage(self):
        self.assertEqual(run_job.main(["Bad Name", "--", "true"]), 2)


if __name__ == "__main__":
    unittest.main()
