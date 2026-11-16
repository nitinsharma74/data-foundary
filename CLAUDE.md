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

# reddit_sentiment_analysis (pyproject sets pytest pythonpath=["src"]; tests import `get_reddit_data` directly)
cd reddit_sentiment_analysis
uv sync --dev                     # dev group = databricks-connect; the tests need the test group instead
UV_PROJECT_ENVIRONMENT=.venv-test uv run --no-default-groups --group test pytest   # needs Java; ~2.5 min
UV_PROJECT_ENVIRONMENT=.venv-test uv run --no-default-groups --group test pytest tests/test_get_reddit_data.py::test_flatten_comments_handles_deep_nesting
uv run python src/get_reddit_data.py --table-name workspace.default.dev_reddit_data  # needs Spark+Delta

# Deploy (either project)
databricks bundle validate
databricks bundle deploy --target dev    # dev is default: resources prefixed, schedules paused
databricks bundle deploy --target prod   # prod un-pauses the job schedule
```

## Architecture notes

- **Job shape is the same in both bundles.** `resources/*_job.yml` defines a serverless `spark_python_task` (environment_version 3) that runs the `src/*.py` script with `--table-name workspace.default.${bundle.target}_<name>`, then a `dashboard_task` that refreshes the Lakeview dashboard defined in `resources/*.lvdash.json` (dashboard YAML pins a SQL `warehouse_id`). Table names are prefixed by target (`dev_`/`prod_`), but both dashboard JSONs hardcode the `prod_` table names, so a dev deployment's dashboard shows prod data (known issue). Renaming a table means updating the dashboard JSON too.
- **Scripts run as plain files on Databricks, not as installed packages.** That is why `weather_hourly_ingestor.py` tries `from src.constants...` first and falls back to `from constants...`. Keep both import paths working. The `[project.scripts] main` entries in pyproject.toml point to modules that don't exist.
- **Write semantics differ between projects.**
  - Weather: delete-then-append per `(station_id, observation_timestamp)`, creating the table on first run.
  - Reddit: `MERGE INTO` on `id` (posts) and `comment_id` (`<table>_comments`). `_validate_table_name` guards the table name because it is interpolated into SQL.
- **Reddit fixture discovery**: `_fixture_path()` walks up from the script file and the cwd looking for `fixtures/reddit_crunchyroll_<N>days.csv`. The file is deployed with the bundle. When the path is under `/Workspace/`, it is read with pandas and then converted to Spark, because Spark can't read workspace files directly. Otherwise `spark.read.csv(..., multiLine=True, escape='"')`, because fields contain embedded newlines and `""` quotes. Comments are a JSON string column of nested `replies`, flattened via `mapInPandas` for Serverless compatibility.
- **Fixture data pipeline (offline)**: `scripts/fetch_reddit_data.py` scrapes r/crunchyroll, scores sentiment with a HuggingFace transformer (`spacesedan/reddit-sentiment-analysis-longformer`), and uses local Ollama models (`llama3.2:1b` for summaries, `deepseek-r1:8b` for device/issue_type classification) to write `reddit_crunchyroll_<DAYS_BACK>days.csv`. `scripts/fix_csv.py [--fix|--stats]` corrects `issue_type`/`device` against `link_flair_text`. Its `CSV_FILE` constant is hardcoded. Copy the result into `reddit_sentiment_analysis/fixtures/`. The CSV columns must match `SCHEMA` in `get_reddit_data.py`.

## Testing notes

- Weather tests stub `pyspark` through `sys.modules` mocks, so they run without Spark.
- Weather tests patch the module-level `SparkSession` through `_patch_spark`. Don't `monkeypatch.setattr` on children of the shared pyspark MagicMock: pytest 9.2+ (pinned in requirements.txt) undoes that with `delattr`, which breaks later tests.
- Reddit tests use the `test` dependency group (OSS `pyspark==3.5.*`, `delta-spark==3.3.*`), which `[tool.uv] conflicts` with `dev` (databricks-connect). Always run them in a separate venv (`UV_PROJECT_ENVIRONMENT=.venv-test`, gitignored): databricks-connect and pyspark both install into `pyspark/`, so switching groups in one `.venv` corrupts it.
- `tests/conftest.py` provides a session-scoped `spark_session` (`local[1]`, UTC, temp warehouse) with Delta enabled only if `delta-spark` is importable (Delta tests `skipif` otherwise). Delta jars download from Maven on first run. It `addPyFile`s `src/get_reddit_data.py` because `mapInPandas` workers don't see pytest's pythonpath.
- Reddit tests read the real fixture CSV. `_validate_table_name` uses `TABLE_NAME_PATTERN.fullmatch`; don't switch back to `match` with `$`, which accepts a trailing newline (covered by `test_validate_table_name_rejects_trailing_newline`).

## Subagents (.claude/agents)

- `bundle-reviewer` (read-only): checks bundle config contracts after changes to `databricks.yml` or `resources/`.
- `spark-delta-reviewer` (read-only): reviews `src/` code for Serverless/Delta correctness.
- `test-writer`: adds and runs tests in each project's test style.
- `bundle-scaffolder`: creates a new bundle project and wires it into CI.
- `docs-maintainer`: keeps the READMEs and this file in sync with the code.

None of these agents deploys or commits.

### When to start them (no need to ask)

Start these agents on your own whenever the trigger applies; the user does not need to name them. Treat this section as standing permission to delegate.

| Trigger | Agent(s) |
|---|---|
| Edited `databricks.yml` or anything in `resources/` | `bundle-reviewer` |
| Edited any `*/src/**/*.py` | `spark-delta-reviewer` |
| New or changed `src/` logic without a covering test, or a bug fix | `test-writer` |
| Asked to add a new pipeline, job, or project folder | `bundle-scaffolder` |
| A finished change touched `src/`, `resources/`, `databricks.yml`, `pyproject.toml`, `scripts/`, or `.github/workflows/` | `docs-maintainer` |
| Asked to "review" or "audit" a bundle | `bundle-reviewer` + `spark-delta-reviewer` |

How to run them:
- **Run read-only reviewers in parallel.** When both `bundle-reviewer` and `spark-delta-reviewer` apply, start them in the same message.
- **Run file-editing agents one at a time.** `test-writer`, `docs-maintainer`, and `bundle-scaffolder` edit files, so don't run two of them on the same project at once. If they must run concurrently, use `isolation: "worktree"`.
- **Order after an edit:** reviewers (parallel) → fix confirmed findings → `test-writer` → `docs-maintainer`.
- **Don't start agents for trivial changes:** typo fixes, comment-only edits, or pure questions/explanations.
- **Check findings before acting on them.** Relay agent findings to the user and verify them before fixing; agent output is not user approval.

## CI/CD (.github/workflows)

- `tests.yml` (every push and PR): job `pytest` runs `hourly_weather_report` tests (Python 3.11, pip + requirements.txt). Job `reddit-pytest` runs `reddit_sentiment_analysis` tests with Java 17 + setup-uv (Python 3.11): `uv run --locked --no-default-groups --group test pytest`. `--locked` means `uv.lock` must be up to date with pyproject.toml.
- `deploy.yml`: on PRs to main, `databricks bundle validate` for both bundles. On push to main, validate and deploy both bundles to `secrets.BUNDLE_TARGET`. A merge to main deploys, and it does not wait for `tests.yml`.
