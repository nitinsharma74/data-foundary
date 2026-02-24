# Reddit Sentiment Analysis

A Databricks ETL project that loads Reddit post data (and nested comments) from a fixture CSV, cleans and normalizes the records with Spark, and writes the results to Delta tables for analytics and dashboard reporting.

## What this project does

- Loads Reddit post data from a local fixture file in `fixtures/`
- Cleans and normalizes post fields (types, booleans, timestamps, HTML entities, nulls)
- Parses and flattens nested comments into a separate comments table
- Upserts both posts and comments into Delta tables in Databricks
- Refreshes a Databricks dashboard after the ETL task completes (via job workflow)

## Data outputs

The pipeline writes two Delta tables using the base table name you pass:

- `<table_name>`: cleaned Reddit post records
- `<table_name>_comments`: flattened comment records with `post_id` and `parent_id`

The ETL uses merge/upsert behavior so reruns update existing rows and insert new rows.

## How the pipeline works

1. Find a fixture file in `fixtures/` matching `reddit_crunchyroll_<days>days.csv`
2. Load the file into Spark
3. Clean and normalize post fields
4. Parse and flatten JSON comments into rows
5. Write/merge posts and comments into Delta tables

## Project structure

- `src/get_reddit_data.py` - main ETL script (CLI entrypoint for Databricks job)
- `fixtures/` - source CSV fixture(s) used by the ETL
- `resources/reddit_job.yml` - Databricks job definition (ETL + dashboard refresh)
- `resources/reddit_dashboard.yml` - dashboard resource config
- `resources/reddit_analysis.lvdash.json` - dashboard definition
- `tests/test_get_reddit_data.py` - unit tests for fixture loading and cleaning logic
- `databricks.yml` - Databricks Asset Bundle configuration

## Running locally (development)

This project is primarily designed to run on Databricks. The unit tests can be run locally, and the full ETL can be run locally only if your environment supports Spark + Delta.

### Install dev dependencies

```bash
cd reddit_sentiment_analysis
uv sync --dev
```

### Run tests

```bash
cd reddit_sentiment_analysis
PYTHONPATH=src uv run pytest tests/test_get_reddit_data.py
```

### Run the ETL script manually (Spark/Delta environment required)

```bash
cd reddit_sentiment_analysis
PYTHONPATH=src uv run python src/get_reddit_data.py --table-name workspace.default.dev_reddit_data
```

## Deploy to Databricks (Asset Bundle)

```bash
cd reddit_sentiment_analysis
databricks bundle validate
databricks bundle deploy --target dev
```

The job definition in `resources/reddit_job.yml` runs the ETL task and then refreshes the dashboard.

## Notes

- The input fixture currently represents Reddit data for the Crunchyroll subreddit (based on the fixture filename pattern).
- Helper scripts for collecting and cleaning Reddit CSV data live in the repo root under `../scripts/`.
