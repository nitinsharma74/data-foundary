from __future__ import annotations

from pathlib import Path
import os
import html
import logging

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

DEFAULT_TABLE_NAME = "reddit_data"
DEFAULT_TARGET = "dev"
FIXTURE_FILENAME = "reddit_netflix_7days.csv"
SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("author", StringType(), True),
        StructField("ups", LongType(), True),
        StructField("downs", LongType(), True),
        StructField("score", LongType(), True),
        StructField("num_comments", LongType(), True),
        StructField("created_utc", TimestampType(), True),
        StructField("upvote_ratio", DoubleType(), True),
        StructField("link_flair_text", StringType(), True),
        StructField("is_self", BooleanType(), True),
        StructField("is_video", BooleanType(), True),
        StructField("url", StringType(), True),
        StructField("permalink", StringType(), True),
    ]
)


def _fixture_path() -> Path:
    candidate_roots = []
    try:
        candidate_roots.append(Path(__file__).resolve())
    except NameError:
        pass
    candidate_roots.append(Path.cwd())

    searched = []
    for root in candidate_roots:
        for parent in [root, *root.parents]:
            candidate = parent / "fixtures" / FIXTURE_FILENAME
            searched.append(candidate)
            if candidate.exists():
                return candidate

    searched_paths = "\n".join(str(path) for path in searched)
    raise FileNotFoundError(
        f"Unable to find fixture file. Paths tried:\n{searched_paths}"
    )


def _load_fixture_dataframe() -> pd.DataFrame:
    fixture_path = _fixture_path()
    logger.info("Loading fixture data from %s", fixture_path)
    return pd.read_csv(fixture_path)


def _normalize_boolean(value: object) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def _clean_reddit_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned["title"] = cleaned["title"].apply(
        lambda value: html.unescape(value).strip() if isinstance(value, str) else value
    )
    cleaned["author"] = cleaned["author"].apply(
        lambda value: value.strip() if isinstance(value, str) else value
    )
    cleaned["link_flair_text"] = cleaned["link_flair_text"].apply(
        lambda value: value.strip() if isinstance(value, str) else value
    )
    cleaned["url"] = cleaned["url"].apply(
        lambda value: value.strip() if isinstance(value, str) else value
    )
    cleaned["permalink"] = cleaned["permalink"].apply(
        lambda value: value.strip() if isinstance(value, str) else value
    )
    cleaned["created_utc"] = pd.to_datetime(
        cleaned["created_utc"], errors="coerce", utc=True
    ).dt.tz_convert(None)

    for column in ["ups", "downs", "score", "num_comments"]:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce").astype("Int64")

    cleaned["upvote_ratio"] = pd.to_numeric(
        cleaned["upvote_ratio"], errors="coerce"
    )
    cleaned["is_self"] = cleaned["is_self"].apply(_normalize_boolean)
    cleaned["is_video"] = cleaned["is_video"].apply(_normalize_boolean)

    cleaned = cleaned.dropna(subset=["id"])
    cleaned = cleaned.drop_duplicates(subset=["id"])
    cleaned = cleaned[SCHEMA.fieldNames()]
    return cleaned


def _resolve_table_name(table_name: str | None) -> str:
    if table_name:
        return table_name

    target = os.getenv("DATABRICKS_BUNDLE_TARGET") or os.getenv("BUNDLE_TARGET") or os.getenv("ENV") or DEFAULT_TARGET
    target = target.strip().lower()
    if target == "prod":
        return "prod_reddit_data"
    return "dev_reddit_data"


def get_reddit_data(table_name: str | None = None) -> int:
    spark = SparkSession.builder.getOrCreate()
    df = _load_fixture_dataframe()
    cleaned_df = _clean_reddit_dataframe(df)

    resolved_table_name = _resolve_table_name(table_name)
    logger.info("Writing %d cleaned rows to table=%s", len(cleaned_df.index), resolved_table_name)
    spark_df = spark.createDataFrame(cleaned_df, schema=SCHEMA)
    spark.sql(f"DROP TABLE IF EXISTS {resolved_table_name}")
    (
        spark_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(resolved_table_name)
    )
    return len(cleaned_df.index)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    get_reddit_data()
