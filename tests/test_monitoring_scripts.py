"""Host-side monitoring scripts (scripts/monitoring/): todo counts and job status files."""

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
