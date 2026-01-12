from __future__ import annotations

import argparse
import html
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F
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

FIXTURE_FILENAME = "reddit_crunchyroll_60days.csv"

SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),
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
        StructField("sentiment_score", DoubleType(), True),
        StructField("comments", StringType(), True),
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


def _load_fixture_dataframe(spark: SparkSession) -> DataFrame:
    fixture_path = _fixture_path()
    logger.info("Loading fixture data from %s", fixture_path)
    if str(fixture_path).startswith("/Workspace/"):
        pandas_df = pd.read_csv(fixture_path)
        pandas_df["created_utc"] = pd.to_datetime(
            pandas_df["created_utc"], errors="coerce", utc=True
        ).dt.tz_convert(None)
        return spark.createDataFrame(pandas_df, schema=SCHEMA)
    return spark.read.csv(str(fixture_path), header=True, schema=SCHEMA)


def _unescape_and_strip(value: str | None) -> str | None:
    if value is None:
        return None
    return html.unescape(value).strip()


def _clean_reddit_dataframe(df: DataFrame) -> DataFrame:
    unescape_udf = F.udf(_unescape_and_strip, StringType())

    def _trim_or_null(col_name: str) -> F.Column:
        return F.when(F.col(col_name).isNotNull(), F.trim(F.col(col_name))).otherwise(
            None
        )

    cleaned = df.select(
        F.col("id"),
        unescape_udf(F.col("title")).alias("title"),
        unescape_udf(F.col("selftext")).alias("selftext"),
        _trim_or_null("author").alias("author"),
        F.col("ups").cast("long").alias("ups"),
        F.col("downs").cast("long").alias("downs"),
        F.col("score").cast("long").alias("score"),
        F.col("num_comments").cast("long").alias("num_comments"),
        F.to_timestamp(F.col("created_utc")).alias("created_utc"),
        F.col("upvote_ratio").cast("double").alias("upvote_ratio"),
        _trim_or_null("link_flair_text").alias("link_flair_text"),
        F.when(
            F.col("is_self").isNull(),
            None,
        )
        .otherwise(
            F.lower(F.trim(F.col("is_self").cast("string"))) == F.lit("true")
        )
        .alias("is_self"),
        F.when(
            F.col("is_video").isNull(),
            None,
        )
        .otherwise(
            F.lower(F.trim(F.col("is_video").cast("string"))) == F.lit("true")
        )
        .alias("is_video"),
        _trim_or_null("url").alias("url"),
        _trim_or_null("permalink").alias("permalink"),
        F.col("sentiment_score").cast("double").alias("sentiment_score"),
        F.col("comments").alias("comments"),
    )

    cleaned = cleaned.filter(F.col("id").isNotNull())
    cleaned = cleaned.dropDuplicates(["id"])
    cleaned = cleaned.select(*SCHEMA.fieldNames())
    return cleaned


def get_reddit_data(table_name: str) -> int:
    spark = SparkSession.builder.getOrCreate()
    df = _load_fixture_dataframe(spark)
    cleaned_df = _clean_reddit_dataframe(df)

    row_count = cleaned_df.count()
    logger.info("Writing %d cleaned rows to table=%s", row_count, table_name)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    (
        cleaned_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    return row_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Load reddit data into Delta table.")
    parser.add_argument("--table-name", default=None)
    args = parser.parse_args()
    if not args.table_name:
        raise ValueError("table_name is required. Pass --table-name.")
    get_reddit_data(table_name=args.table_name)
