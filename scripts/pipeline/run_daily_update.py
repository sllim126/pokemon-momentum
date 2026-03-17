import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path("/app")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Pokemon Momentum daily update pipeline."
    )
    parser.add_argument(
        "--limit-days",
        type=int,
        default=0,
        help="Only load up to N missing days into DuckDB.",
    )
    parser.add_argument(
        "--latest-first",
        action="store_true",
        help="Load newest missing days first when building the DuckDB price table.",
    )
    parser.add_argument(
        "--refresh-csv",
        action="store_true",
        help="Also rebuild pokemon_prices_all_days.csv during the price-load step.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading/extracting new archives.",
    )
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Skip parquet export.",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip group/product metadata refresh and joined-name export.",
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip ranking and indicator snapshot steps.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going after a failed step instead of stopping immediately.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the pipeline commands without running them.",
    )
    return parser.parse_args()


def build_price_load_command(args: argparse.Namespace) -> list[str]:
    cmd = ["python", "scripts/extract/build_pokemon_prices_all_days.py"]
    if args.limit_days > 0:
        cmd.extend(["--limit-days", str(args.limit_days)])
    if args.latest_first:
        cmd.append("--latest-first")
    if args.refresh_csv:
        cmd.append("--refresh-csv")
    return cmd


def build_steps(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    steps: list[tuple[str, list[str]]] = []

    if not args.skip_download:
        steps.append(("Download and extract new archives", ["python", "scripts/download/Download_new_day.py"]))

    steps.append(("Load new price data into DuckDB", build_price_load_command(args)))

    if not args.skip_metadata:
        steps.extend(
            [
                ("Refresh group metadata", ["python", "scripts/utilities/export_pokemon_groups.py"]),
                ("Refresh product metadata", ["python", "scripts/utilities/export_products_for_my_groups.py"]),
                ("Rebuild joined price/name export", ["python", "scripts/utilities/join_prices_to_names.py"]),
            ]
        )

    if not args.skip_analytics:
        steps.extend(
            [
                ("Build top-200 universe", ["python", "scripts/rankings/top_200.py"]),
                ("Build top-200 lookup", ["python", "scripts/rankings/make_top200_lookup.py"]),
                ("Build top-200 indicators", ["python", "scripts/indicators/compute_200_indicators.py"]),
                ("Build top-200 named movers", ["python", "scripts/rankings/top200_with_names.py"]),
                ("Build top-200 timeseries", ["python", "scripts/rankings/build_top200_timeseries.py"]),
                ("Build ROC 7/30/90 snapshot", ["python", "scripts/indicators/compute_roc_7_30_90.py"]),
                ("Build product signal snapshot", ["python", "scripts/indicators/build_product_signal_snapshot.py"]),
            ]
        )

    if not args.skip_parquet:
        steps.append(("Export parquet partitions", ["python", "scripts/utilities/export_parquet.py"]))

    return steps


def run_step(name: str, cmd: list[str], dry_run: bool) -> int:
    rendered = " ".join(shlex.quote(part) for part in cmd)
    print(f"\n=== {name} ===")
    print(rendered)

    if dry_run:
        return 0

    started = time.time()
    proc = subprocess.run(cmd, cwd=ROOT)
    elapsed = time.time() - started
    print(f"--- exit={proc.returncode} elapsed={elapsed:.1f}s ---")
    return proc.returncode


def main() -> int:
    args = parse_args()
    steps = build_steps(args)

    print("Pokemon Momentum daily update pipeline")
    print(f"Working directory: {ROOT}")
    print(f"Steps queued: {len(steps)}")

    failures: list[tuple[str, int]] = []

    for name, cmd in steps:
        code = run_step(name, cmd, args.dry_run)
        if code == 0:
            continue
        failures.append((name, code))
        if not args.continue_on_error:
            print(f"Stopping after failed step: {name}")
            break

    if failures:
        print("\nPipeline finished with failures:")
        for name, code in failures:
            print(f"- {name}: exit {code}")
        return 1

    print("\nPipeline finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
