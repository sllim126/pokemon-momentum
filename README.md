# pokemon-momentum
Momentum analysis and data pipeline for Pokemon TCG market data

## Docker rebuild and run

This project is set up to run inside Docker with the repo mounted into the container at `/app`.

Build the image:

```bash
docker compose build
```

Start the container:

```bash
docker compose up -d
```

Open a shell in the running container:

```bash
docker compose exec pokemon-momentum bash
```

The project uses these folders inside the container:

- `/app/data/raw` for downloaded `.7z` archives
- `/app/data/extracted` for extracted data files and generated CSVs used by the pipeline
- `/app/output` for generated reports and HTML dashboards

If you need a clean rebuild on a new Linux system:

1. Clone the repository.
2. Run `docker compose build`.
3. Run `docker compose up -d`.
4. Run the pipeline commands below from inside the container.

## Current workflow

Run these commands from inside the container shell opened with `docker compose exec pokemon-momentum bash`.

1. Download and extract new daily archive files:

```bash
python scripts/download/Download_new_day.py
```

2. Build the consolidated price history CSV:

```bash
python scripts/extract/build_pokemon_prices_all_days.py
```

3. Refresh set metadata:

```bash
python scripts/utilities/export_pokemon_groups.py
```

4. Refresh product metadata for the groups found in the price history:

```bash
python scripts/utilities/export_products_for_my_groups.py
```

5. Join prices, groups, and products:

```bash
python scripts/utilities/join_prices_to_names.py
```

6. Optional single-card moving average export:

```bash
python scripts/indicators/single_card_moving_average.py
```

7. Build the top-200 universe:

```bash
python scripts/rankings/top_200.py
```

8. Compute indicators for the top-200 universe:

```bash
python scripts/indicators/compute_200_indicators.py
```

9. Add names to the top-200 output:

```bash
python scripts/rankings/top_200_with_names.py
```

10. Build the top-200 dashboard:

```bash
python scripts/dashboards/build_dashboard_html.py
```

11. Build the top-200 timeseries file:

```bash
python scripts/rankings/build_top200_timeseries.py
```

12. Build the 7/30/90 ROC snapshot:

```bash
python scripts/indicators/compute_roc_7_30_90.py
```

13. Build the ROC dashboard:

```bash
python scripts/dashboards/build_roc_dashboard_v3.py
```

Generated CSVs and dashboards are rebuildable outputs and should not be committed to Git.

## Automated daily update

Use the pipeline runner inside the container to execute the working update sequence in order:

```bash
python scripts/pipeline/run_daily_update.py
```

Useful flags:

```bash
# Catch up only the newest 12 missing days
python scripts/pipeline/run_daily_update.py --latest-first --limit-days 12

# Skip network-heavy metadata refresh
python scripts/pipeline/run_daily_update.py --skip-metadata

# Skip parquet export
python scripts/pipeline/run_daily_update.py --skip-parquet

# Print commands without running them
python scripts/pipeline/run_daily_update.py --dry-run
```

Current automated sequence:

1. Download and extract new archives
2. Load new price data into DuckDB
3. Refresh group metadata
4. Refresh product metadata
5. Rebuild joined name export
6. Build top-200 universe
7. Build top-200 lookup
8. Build top-200 indicators
9. Build top-200 named movers
10. Build top-200 timeseries
11. Build ROC 7/30/90 snapshot
12. Build product signal snapshot
13. Export parquet partitions
