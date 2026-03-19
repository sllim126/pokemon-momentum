import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.common.category_config import get_category_config


ROOT = Path("/app")


def parse_args() -> argparse.Namespace:
    """Parse the CLI options that shape one pipeline run.

    Expected result:
    - identifies which category to process
    - determines which stages are skipped or forced
    - controls whether the run is a cheap incremental pass or a broader rebuild
    """
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
        "--category-id",
        type=int,
        default=3,
        help="Category ID to process. Default: 3 (Pokemon).",
    )
    parser.add_argument(
        "--latest-first",
        action="store_true",
        help="Load newest missing days first when building the DuckDB price table.",
    )
    parser.add_argument(
        "--refresh-csv",
        action="store_true",
        help="Also rebuild the category price CSV during the price-load step.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Worker count for the DuckDB price-load step. Keep this modest for daily incremental runs.",
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
        "--full-metadata-refresh",
        action="store_true",
        help="Force a full product metadata refresh instead of incremental metadata fetches.",
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip product/group indicator snapshot steps.",
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
    """Build the history-load command for the selected category.

    Expected result:
    - returns a command that loads any missing daily price rows into pokemon_prices
    - optionally refreshes the category CSV for manual inspection or legacy compatibility
    """
    cmd = ["python", "scripts/extract/build_pokemon_prices_all_days.py"]
    cmd.extend(["--category-id", str(args.category_id)])
    cmd.extend(["--workers", str(max(1, args.workers))])
    if args.refresh_csv:
        cmd.append("--refresh-csv")
    if args.limit_days > 0:
        cmd.extend(["--limit-days", str(args.limit_days)])
    if args.latest_first:
        cmd.append("--latest-first")
    return cmd


def build_steps(args: argparse.Namespace) -> list[tuple[str, list[str]]]:
    """Assemble the ordered pipeline stages for this run.

    Expected result:
    - each tuple describes one stage and the command that should produce its artifacts
    - later stages assume earlier stages completed successfully
    """
    steps: list[tuple[str, list[str]]] = []
    category = get_category_config(args.category_id)

    if not args.skip_download:
        # Expected result: new daily archives are downloaded/extracted when they exist upstream.
        steps.append(("Download and extract new archives", ["python", "scripts/download/Download_new_day.py"]))

    # Expected result: pokemon_prices contains all missing dates for the selected category.
    steps.append(("Load new price data into DuckDB", build_price_load_command(args)))

    if not args.skip_metadata:
        product_refresh_cmd = [
            "python",
            "scripts/utilities/export_products_for_my_groups.py",
            "--category-id",
            str(args.category_id),
        ]
        if args.full_metadata_refresh:
            product_refresh_cmd.append("--full-refresh")
        steps.extend(
            [
                # Expected result: category-specific set/group metadata exists in DuckDB and CSV form.
                (
                    "Refresh group metadata",
                    ["python", "scripts/utilities/export_pokemon_groups.py", "--category-id", str(args.category_id)],
                ),
                # Expected result: category-specific product metadata exists in DuckDB and CSV form.
                ("Refresh product metadata", product_refresh_cmd),
                # Expected result: a joined price/name export is available for inspection and downstream use.
                (
                    "Rebuild joined price/name export",
                    ["python", "scripts/utilities/join_prices_to_names.py", "--category-id", str(args.category_id)],
                ),
            ]
        )

    if not args.skip_analytics:
        steps.extend(
            [
                (
                    # Expected result: product-level momentum snapshot exists for API and screener reads.
                    f"Build {category.label} product signal snapshot",
                    ["python", "scripts/indicators/build_product_signal_snapshot.py", "--category-id", str(args.category_id)],
                ),
                (
                    # Expected result: group-level breadth/momentum snapshot exists for set ranking views.
                    f"Build {category.label} group signal snapshot",
                    ["python", "scripts/indicators/build_group_signal_snapshot.py", "--category-id", str(args.category_id)],
                ),
            ]
        )

    if not args.skip_parquet:
        # Expected result: parquet history includes the latest processed dates for downstream reads.
        steps.append(("Export parquet partitions", ["python", "scripts/utilities/export_parquet.py"]))

    return steps


def run_step(name: str, cmd: list[str], dry_run: bool) -> int:
    """Execute one pipeline step and log its timing.

    Expected result:
    - returns the subprocess exit code
    - prints the exact command so failures can be traced from logs alone
    """
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
    """Run the full ordered pipeline and stop on the first failure by default.

    Expected result:
    - returns 0 when every queued stage succeeds
    - returns 1 when any stage fails, with the failing step reported to stdout
    """
    args = parse_args()
    steps = build_steps(args)
    category = get_category_config(args.category_id)

    print("Pokemon Momentum daily update pipeline")
    print(f"Working directory: {ROOT}")
    print(f"Category: {category.label} ({category.category_id})")
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
