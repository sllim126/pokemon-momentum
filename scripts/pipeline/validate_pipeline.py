import argparse
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.common.category_config import get_category_config


ROOT = Path("/app")
EXTRACTED_DIR = ROOT / "data" / "extracted"
PROCESSED_DIR = ROOT / "data" / "processed"
PARQUET_ROOT = ROOT / "data" / "parquet"
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"


@dataclass(frozen=True)
class StepSpec:
    """One pipeline stage plus the outputs that should exist afterward."""
    name: str
    command: list[str]
    file_outputs: tuple[Path, ...] = ()
    table_outputs: tuple[str, ...] = ()
    requires_parquet: bool = False


def parse_args() -> argparse.Namespace:
    """Parse validator options.

    Expected result:
    - supports validate-only mode or run-and-validate mode
    - allows targeting a specific category or subset of stages
    """
    parser = argparse.ArgumentParser(
        description="Validate or execute the Pokemon Momentum pipeline step-by-step."
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Execute each step before validating its outputs. Default is validate-only.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep validating later steps after a failure.",
    )
    parser.add_argument(
        "--from-step",
        help="Start at the first step whose name contains this text (case-insensitive).",
    )
    parser.add_argument(
        "--step",
        action="append",
        default=[],
        help="Only run/validate steps whose names contain this text (can be used multiple times).",
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="Category ID to validate. Default: 3 (Pokemon).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Worker count to use for the price-load step when --run is enabled.",
    )
    parser.add_argument(
        "--full-metadata-refresh",
        action="store_true",
        help="Use --full-refresh for the product metadata step when --run is enabled.",
    )
    return parser.parse_args()


def build_steps(category_id: int, workers: int, full_metadata_refresh: bool) -> list[StepSpec]:
    """Build the expected pipeline plan for a category.

    Expected result:
    - each StepSpec pairs a command with the files/tables/parquet state it should produce
    - validation follows the active screener pipeline, not legacy top-200 compatibility steps
    """
    category = get_category_config(category_id)
    product_refresh_cmd = [
        "python",
        "scripts/utilities/export_products_for_my_groups.py",
        "--category-id",
        str(category_id),
    ]
    if full_metadata_refresh:
        product_refresh_cmd.append("--full-refresh")

    steps = [
        StepSpec(
            # Expected result: category rows exist in pokemon_prices.
            name="Load new price data into DuckDB",
            command=[
                "python",
                "scripts/extract/build_pokemon_prices_all_days.py",
                "--category-id",
                str(category_id),
                "--workers",
                str(max(1, workers)),
            ],
            table_outputs=("pokemon_prices",),
        ),
        StepSpec(
            # Expected result: category-specific group metadata exists as table and CSV.
            name="Refresh group metadata",
            command=["python", "scripts/utilities/export_pokemon_groups.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.groups_csv,),
            table_outputs=(category.groups_table,),
        ),
        StepSpec(
            # Expected result: category-specific product metadata exists as table and CSV.
            name="Refresh product metadata",
            command=product_refresh_cmd,
            file_outputs=(EXTRACTED_DIR / category.products_csv,),
            table_outputs=(category.products_table,),
        ),
        StepSpec(
            # Expected result: joined price/name export exists for easier debugging and downstream use.
            name="Rebuild joined price/name export",
            command=["python", "scripts/utilities/join_prices_to_names.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.prices_named_csv,),
            table_outputs=(category.prices_named_table,),
        ),
        StepSpec(
            # Expected result: lightweight health summary exists for fast dashboard status reads.
            name="Build health snapshot",
            command=["python", "scripts/indicators/build_health_snapshot.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.health_snapshot_csv,),
            table_outputs=(category.health_snapshot_table,),
        ),
        StepSpec(
            # Expected result: product-level signal snapshot exists for screeners and API calls.
            name="Build product signal snapshot",
            command=["python", "scripts/indicators/build_product_signal_snapshot.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.product_signal_csv,),
            table_outputs=(category.product_signal_table,),
        ),
        StepSpec(
            # Expected result: group-level signal snapshot exists for breadth/set views.
            name="Build group signal snapshot",
            command=["python", "scripts/indicators/build_group_signal_snapshot.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.group_signal_csv,),
            table_outputs=(category.group_signal_table,),
        ),
        StepSpec(
            # Expected result: compact sparkline payloads exist for ticker / mini-chart requests.
            name="Build sparkline snapshot",
            command=["python", "scripts/indicators/build_sparkline_snapshot.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.sparkline_snapshot_csv,),
            table_outputs=(category.sparkline_snapshot_table,),
        ),
        StepSpec(
            # Expected result: compact chart-series payloads exist for normal dashboard chart windows.
            name="Build series snapshot",
            command=["python", "scripts/indicators/build_series_snapshot.py", "--category-id", str(category_id)],
            file_outputs=(EXTRACTED_DIR / category.series_snapshot_csv,),
            table_outputs=(category.series_snapshot_table,),
        ),
        StepSpec(
            # Expected result: parquet partitions exist and include the most recent processed date.
            name="Export parquet partitions",
            command=["python", "scripts/utilities/export_parquet.py"],
            requires_parquet=True,
        ),
    ]

    return steps


def select_steps(steps: list[StepSpec], args: argparse.Namespace) -> list[StepSpec]:
    """Filter the validation plan down to the user-requested subset of steps."""
    selected = steps
    if args.from_step:
        needle = args.from_step.lower()
        for idx, step in enumerate(steps):
            if needle in step.name.lower():
                selected = steps[idx:]
                break
        else:
            selected = []

    if args.step:
        needles = [needle.lower() for needle in args.step]
        selected = [
            step for step in selected
            if any(needle in step.name.lower() for needle in needles)
        ]
    return selected


def table_row_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def validate_step(step: StepSpec) -> tuple[bool, list[str]]:
    """Verify that one stage produced the artifacts it was supposed to produce.

    Expected result:
    - PASS if every declared output exists and is non-empty
    - FAIL if any file/table/parquet expectation is missing or empty
    """
    messages: list[str] = []

    for path in step.file_outputs:
        if not path.exists():
            messages.append(f"missing file: {path}")
            continue
        size = path.stat().st_size
        if size <= 0:
            messages.append(f"empty file: {path}")
        else:
            messages.append(f"file ok: {path} ({size:,} bytes)")

    con = duckdb.connect(str(DB_PATH), read_only=True) if DB_PATH.exists() else None
    try:
        if con is not None:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            for table_name in step.table_outputs:
                if table_name not in tables:
                    messages.append(f"missing table: {table_name}")
                    continue
                row_count = table_row_count(con, table_name)
                if row_count <= 0:
                    messages.append(f"empty table: {table_name}")
                else:
                    messages.append(f"table ok: {table_name} ({row_count:,} rows)")
        elif step.table_outputs:
            for table_name in step.table_outputs:
                messages.append(f"database missing; cannot verify table: {table_name}")

        if step.requires_parquet:
            if not PARQUET_ROOT.exists():
                messages.append(f"missing parquet root: {PARQUET_ROOT}")
            else:
                partitions = sorted([p for p in PARQUET_ROOT.iterdir() if p.is_dir() and p.name.startswith("date=")])
                if not partitions:
                    messages.append("no parquet partitions found")
                else:
                    newest = partitions[-1].name.split("=", 1)[1]
                    messages.append(f"parquet ok: {len(partitions):,} partitions, latest={newest}")
    finally:
        if con is not None:
            con.close()

    failed = any(msg.startswith(("missing", "empty", "database missing", "no parquet")) for msg in messages)
    return (not failed), messages


def run_step(step: StepSpec) -> int:
    """Run one stage command before validating its outputs."""
    rendered = " ".join(shlex.quote(part) for part in step.command)
    print(f"\n=== RUN {step.name} ===")
    print(rendered)
    started = time.time()
    proc = subprocess.run(step.command, cwd=ROOT)
    elapsed = time.time() - started
    print(f"--- exit={proc.returncode} elapsed={elapsed:.1f}s ---")
    return proc.returncode


def main() -> int:
    """Run the validator and print a per-stage PASS/FAIL summary.

    Expected result:
    - returns 0 when all requested stages validate successfully
    - returns 1 when any stage fails to run or validate
    """
    args = parse_args()
    category = get_category_config(args.category_id)
    steps = select_steps(build_steps(args.category_id, args.workers, args.full_metadata_refresh), args)
    if not steps:
        print("No steps matched.")
        return 1

    print(f"Category: {category.label} ({category.category_id})")

    failures: list[str] = []

    for step in steps:
        if args.run:
            code = run_step(step)
            if code != 0:
                failures.append(f"{step.name}: command failed with exit {code}")
                print(f"FAIL: {step.name}")
                if not args.continue_on_error:
                    break
                continue

        ok, messages = validate_step(step)
        status = "PASS" if ok else "FAIL"
        print(f"\n=== {status} {step.name} ===")
        for message in messages:
            print(f"- {message}")
        if not ok:
            failures.append(f"{step.name}: validation failed")
            if not args.continue_on_error:
                break

    if failures:
        print("\nValidation finished with failures:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("\nValidation finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
