from __future__ import annotations

from pathlib import Path
import io
import os
import sys
import trace
import unittest
from contextlib import redirect_stdout


REPO_ROOT = Path(__file__).resolve().parents[1]
MIN_COVERAGE = float(os.environ.get("MIN_COVERAGE", "90"))
TARGET_MODULES = [
    REPO_ROOT / "scripts" / "common" / "category_config.py",
    REPO_ROOT / "scripts" / "common" / "product_classification.py",
    REPO_ROOT / "scripts" / "dashboards" / "query_support.py",
]


def discover_suite() -> unittest.TestSuite:
    loader = unittest.TestLoader()
    return loader.discover(str(REPO_ROOT / "tests"), pattern="test_*.py")


def executable_line_numbers(path: Path) -> set[int]:
    return set(trace._find_executable_linenos(str(path)))  # noqa: SLF001 - stdlib helper is enough here


def line_hits_for(results: trace.CoverageResults, path: Path) -> dict[int, int]:
    resolved = str(path.resolve())
    hits: dict[int, int] = {}
    for (filename, lineno), count in results.counts.items():
        if Path(filename).resolve() == Path(resolved):
            hits[lineno] = hits.get(lineno, 0) + count
    return hits


def compute_summary(results: trace.CoverageResults) -> tuple[list[dict[str, float | int | str]], float]:
    rows = []
    total_lines = 0
    total_hits = 0
    for module_path in TARGET_MODULES:
        executable = executable_line_numbers(module_path)
        hits = line_hits_for(results, module_path)
        hit_count = sum(1 for lineno in executable if hits.get(lineno, 0) > 0)
        line_count = len(executable)
        percent = (hit_count / line_count * 100.0) if line_count else 100.0
        rows.append(
            {
                "module": str(module_path.relative_to(REPO_ROOT)),
                "lines": line_count,
                "hits": hit_count,
                "percent": percent,
            }
        )
        total_lines += line_count
        total_hits += hit_count
    overall = (total_hits / total_lines * 100.0) if total_lines else 100.0
    return rows, overall


def main() -> int:
    os.chdir(REPO_ROOT)
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    tracer = trace.Trace(
        count=True,
        trace=False,
        ignoredirs=[sys.prefix, sys.exec_prefix],
    )
    stream = io.StringIO()

    def run_suite():
        # Discover inside the traced function so import-time lines in the target modules count
        # toward coverage instead of looking artificially "missed" during test loading.
        suite = discover_suite()
        return unittest.TextTestRunner(verbosity=2).run(suite)

    with redirect_stdout(stream):
        result = tracer.runfunc(run_suite)
    print(stream.getvalue(), end="")
    coverage_rows, overall = compute_summary(tracer.results())
    print("\nCoverage summary (target modules)")
    for row in coverage_rows:
        print(
            f"- {row['module']}: {row['hits']}/{row['lines']} lines "
            f"({row['percent']:.1f}%)"
        )
    print(f"Overall target coverage: {overall:.1f}%")
    if not result.wasSuccessful():
        return 1
    if overall < MIN_COVERAGE:
        print(f"Coverage check failed: required >= {MIN_COVERAGE:.1f}%")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
