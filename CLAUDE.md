# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

A monorepo of independent Databricks ETL projects, each its own **Databricks Asset Bundle** with its own `databricks.yml`, `pyproject.toml`, `src/`, `tests/`, and `resources/`. Always `cd` into a project directory before running uv, pytest, or databricks commands; there is no root-level Python project.

- `hourly_weather_report/`: pulls the latest NOAA observations (api.weather.gov) for the stations in `src/constants/weather_constants.py` and writes them to a Delta table.
- `reddit_sentiment_analysis/`: loads a pre-built Reddit CSV from `fixtures/`, cleans it with Spark, and upserts posts plus flattened comments into two Delta tables.
- `scripts/`: offline helpers, not deployed. They produce the Reddit fixture CSV (see below).

## Commands

```bash
# hourly_weather_report (pytest.ini sets pythonpath=.; tests import `from src import ...`)
cd hourly_weather_report
uv sync --dev                     # or: pip install -r requirements.txt (what CI uses)
uv run pytest
uv run pytest tests/test_weather_hourly_ingestor.py::test_fetch_hourly_weather_parses_rows

# reddit_sentiment_analysis (tests import `get_reddit_data` directly, so PYTHONPATH=src is required)
cd reddit_sentiment_analysis
uv sync --dev
PYTHONPATH=src uv run pytest tests/test_get_reddit_data.py
PYTHONPATH=src uv run python src/get_reddit_data.py --table-name workspace.default.dev_reddit_data  # needs Spark+Delta

# Deploy (either project)
databricks bundle validate
databricks bundle deploy --target dev    # dev is default: resources prefixed, schedules paused
databricks bundle deploy --target prod   # prod un-pauses the job schedule
```

## Architecture notes

- **Job shape is the same in both bundles.** `resources/*_job.yml` defines a serverless `spark_python_task` (environment_version 3) that runs the `src/*.py` script with `--table-name workspace.default.${bundle.target}_<name>`, then a `dashboard_task` that refreshes the Lakeview dashboard defined in `resources/*.lvdash.json` (dashboard YAML pins a SQL `warehouse_id`). Table names are prefixed by target (`dev_`/`prod_`), and the dashboard JSON queries those tables, so renaming a table means updating the dashboard JSON too.
- **Scripts run as plain files on Databricks, not as installed packages.** That is why `weather_hourly_ingestor.py` tries `from src.constants...` first and falls back to `from constants...`. Keep both import paths working. The `[project.scripts] main` entries in pyproject.toml point to modules that don't exist.
- **Write semantics differ between projects.**
  - Weather: delete-then-append per `(station_id, observation_timestamp)`, creating the table on first run.
  - Reddit: `MERGE INTO` on `id` (posts) and `comment_id` (`<table>_comments`). `_validate_table_name` guards the table name because it is interpolated into SQL.
- **Reddit fixture discovery**: `_fixture_path()` walks up from the script file and the cwd looking for `fixtures/reddit_crunchyroll_<N>days.csv`. The file is deployed with the bundle. When the path is under `/Workspace/`, it is read with pandas and then converted to Spark, because Spark can't read workspace files directly. Comments are a JSON string column of nested `replies`, flattened via `mapInPandas` for Serverless compatibility.
- **Fixture data pipeline (offline)**: `scripts/fetch_reddit_data.py` scrapes r/crunchyroll, scores sentiment with a HuggingFace transformer (`spacesedan/reddit-sentiment-analysis-longformer`), and uses local Ollama models (`llama3.2:1b` for summaries, `deepseek-r1:8b` for device/issue_type classification) to write `reddit_crunchyroll_<DAYS_BACK>days.csv`. `scripts/fix_csv.py [--fix|--stats]` corrects `issue_type`/`device` against `link_flair_text`. Its `CSV_FILE` constant is hardcoded. Copy the result into `reddit_sentiment_analysis/fixtures/`. The CSV columns must match `SCHEMA` in `get_reddit_data.py`.

## Testing notes

- Weather tests stub `pyspark` through `sys.modules` mocks, so they run without Spark.
- Reddit tests start a real local `SparkSession` (needs Java + pyspark, which come from `databricks-connect` in the dev group) and read the real fixture CSV.

## CI/CD (.github/workflows)

- `tests.yml`: runs pytest **only for `hourly_weather_report`** (Python 3.11, requirements.txt). Reddit tests are not run in CI.
- `deploy.yml`: on PRs to main, `databricks bundle validate` for both bundles. On push to main, validate and deploy both bundles to `secrets.BUNDLE_TARGET`. A merge to main deploys.
