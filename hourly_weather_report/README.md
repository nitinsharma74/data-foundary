# Hourly Weather ETL Project

An end-to-end data engineering project that ingests hourly NOAA weather observations, stores them in Delta Lake on Databricks, and refreshes a dashboard on a schedule. The pipeline is designed to be small, readable, and easy to extend with new weather stations.

## Highlights

- Multi-station ingestion (KJFK, KSFO, KDFW) with a simple list-based config
- Delta Lake writes with idempotent updates per station/timestamp
- Automated Databricks job + dashboard refresh
- Unit tests for core ingestion logic

## Architecture

1. Fetch latest observations from NOAA for each configured station.
2. Normalize the response into a single-row schema.
3. Write into a Delta table in Databricks.
4. Refresh the dashboard after each run.

## Tech stack

- Python + pandas
- Databricks + Delta Lake
- Databricks Asset Bundles
- pytest

## Project layout

- `src/weather_hourly_ingestor.py` ETL logic
- `src/constants/weather_constants.py` configuration
- `resources/weather_job.yml` job scheduling
- `resources/weather_dashboard.yml` dashboard resource
- `resources/weather_dashboard.lvdash.json` dashboard definition
- `tests/test_weather_hourly_ingestor.py` tests

## Run locally

```bash
uv sync --dev
uv run pytest
```

## Deploy to Databricks

```bash
databricks configure
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

## Configuration

- Add stations in `src/constants/weather_constants.py` via `WEATHER_STATION_IDS`.
- Update the target table with `WEATHER_TABLE_NAME`.
- Adjust schedule in `resources/weather_job.yml`.

## Results

The pipeline produces a Delta table with station metadata, observation timestamps, and weather metrics, which powers the dashboard for quick hourly insights.

## Next ideas

- Add more stations or regions
- Backfill historical observations
- Add data quality checks
