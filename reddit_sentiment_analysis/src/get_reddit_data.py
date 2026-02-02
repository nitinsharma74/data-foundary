from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Iterator

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

FIXTURE_FILENAME_PATTERN = re.compile(r"reddit_crunchyroll_\d+days\.csv")
DATABRICKS_WORKSPACE_PREFIX = "/Workspace/"
TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")

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
        StructField("sentiment_score_transformer", DoubleType(), True),
        StructField("sentiment_bucket_transformer", StringType(), True),
        StructField("device", StringType(), True),
        StructField("issue_type", StringType(), True),
        StructField("post_summary", StringType(), True),
        StructField("comments", StringType(), True),
    ]
)

COMMENTS_SCHEMA = StructType(
    [
        StructField("comment_id", StringType(), False),
        StructField("post_id", StringType(), False),
        StructField("parent_id", StringType(), True),
        StructField("author", StringType(), True),
        StructField("body", StringType(), True),
        StructField("score", LongType(), True),
        StructField("created_utc", TimestampType(), True),
    ]
)


def _fixture_path() -> Path:
    """Locate the fixture CSV file by searching candidate directories.

    Searches from the script's directory and current working directory,
    looking for a 'fixtures' subdirectory containing a file matching
    the pattern reddit_crunchyroll_{days}days.csv.

    Returns:
        Path to the fixture file.

    Raises:
        FileNotFoundError: If the fixture file cannot be found.
    """
    candidate_roots = []
    try:
        candidate_roots.append(Path(__file__).resolve())
    except NameError:
        pass
    candidate_roots.append(Path.cwd())

    searched = []
    for root in candidate_roots:
        for parent in [root, *root.parents]:
            fixtures_dir = parent / "fixtures"
            searched.append(fixtures_dir)
            if fixtures_dir.exists() and fixtures_dir.is_dir():
                for candidate in fixtures_dir.iterdir():
                    if candidate.is_file() and FIXTURE_FILENAME_PATTERN.match(candidate.name):
                        return candidate

    searched_paths = "\n".join(str(path) for path in searched)
    raise FileNotFoundError(
        f"Unable to find fixture file matching pattern '{FIXTURE_FILENAME_PATTERN.pattern}'. "
        f"Directories searched:\n{searched_paths}"
    )


def _load_fixture_dataframe(spark: SparkSession) -> DataFrame:
    """Load the fixture CSV file into a Spark DataFrame.

    Handles both local filesystem and Databricks Workspace paths.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame containing the raw fixture data.
    """
    fixture_path = _fixture_path()
    logger.info(f"Loading fixture data from {fixture_path}")
    if str(fixture_path).startswith(DATABRICKS_WORKSPACE_PREFIX):
        pandas_df = pd.read_csv(fixture_path)
        pandas_df["created_utc"] = pd.to_datetime(
            pandas_df["created_utc"], errors="coerce", utc=True
        ).dt.tz_convert(None)
        return spark.createDataFrame(pandas_df, schema=SCHEMA)
    return spark.read.csv(str(fixture_path), header=True, schema=SCHEMA)


def _unescape_html(col: F.Column) -> F.Column:
    """Unescape common HTML entities and strip whitespace using native Spark functions.

    Replaces common HTML entities with their character equivalents:
    - &amp; -> &
    - &lt; -> <
    - &gt; -> >
    - &quot; -> "
    - &#39; or &apos; -> '
    - &nbsp; -> space

    Args:
        col: Spark Column containing text with HTML entities.

    Returns:
        Column with HTML entities replaced and whitespace trimmed.
    """
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(col, "&amp;", "&"),
                            "&lt;", "<"
                        ),
                        "&gt;", ">"
                    ),
                    "&quot;", '"'
                ),
                "&#39;|&apos;", "'"
            ),
            "&nbsp;", " "
        )
    )


def _flatten_comments(
    comments: list[dict[str, Any]], post_id: str, parent_id: str | None = None
) -> Iterator[dict[str, Any]]:
    """Recursively flatten nested comments into flat rows.

    Args:
        comments: List of comment dictionaries with nested 'replies'.
        post_id: ID of the parent post.
        parent_id: ID of the parent comment (None for top-level comments).

    Yields:
        Flattened comment dictionaries with post_id and parent_id.
    """
    for comment in comments:
        yield {
            "comment_id": comment.get("id"),
            "post_id": post_id,
            "parent_id": parent_id,
            "author": comment.get("author"),
            "body": comment.get("body"),
            "score": comment.get("score"),
            "created_utc": comment.get("created_utc"),
        }
        replies = comment.get("replies", [])
        if replies:
            yield from _flatten_comments(replies, post_id, parent_id=comment.get("id"))


def _extract_comments_dataframe(spark: SparkSession, posts_df: DataFrame) -> DataFrame:
    """Extract and flatten comments from posts DataFrame.

    Parses the JSON comments column, recursively flattens nested replies,
    and returns a cleaned DataFrame with proper parent-child relationships.

    Uses mapInPandas for Databricks Serverless compatibility.

    Args:
        spark: Active SparkSession.
        posts_df: DataFrame containing posts with 'id' and 'comments' columns.

    Returns:
        DataFrame with flattened comments matching COMMENTS_SCHEMA.
    """
    posts_with_comments = posts_df.select(
        F.col("id").alias("post_id"),
        F.col("comments"),
    ).filter(
        F.col("comments").isNotNull() & (F.trim(F.col("comments")) != "")
    )

    # Check if empty using limit(1) instead of count() for efficiency
    if posts_with_comments.limit(1).count() == 0:
        logger.info("No comments found to extract")
        return spark.createDataFrame([], schema=COMMENTS_SCHEMA)

    # Define the output schema for mapInPandas
    comments_output_schema = StructType([
        StructField("comment_id", StringType(), True),
        StructField("post_id", StringType(), True),
        StructField("parent_id", StringType(), True),
        StructField("author", StringType(), True),
        StructField("body", StringType(), True),
        StructField("score", LongType(), True),
        StructField("created_utc", StringType(), True),
    ])

    def parse_and_flatten_pandas(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Parse JSON and flatten comments for batches of posts using pandas."""
        for pdf in iterator:
            all_comments = []
            for _, row in pdf.iterrows():
                post_id = row["post_id"]
                comments_json = row["comments"]
                try:
                    comments_list = json.loads(comments_json)
                    if isinstance(comments_list, list):
                        all_comments.extend(_flatten_comments(comments_list, post_id))
                except json.JSONDecodeError:
                    continue
            if all_comments:
                yield pd.DataFrame(all_comments)
            else:
                # Yield empty DataFrame with correct columns
                yield pd.DataFrame(columns=[
                    "comment_id", "post_id", "parent_id", "author", "body", "score", "created_utc"
                ])

    # Use mapInPandas for Databricks Serverless compatibility
    comments_df = posts_with_comments.mapInPandas(
        parse_and_flatten_pandas, schema=comments_output_schema
    )

    # Filter out empty results and apply cleaning
    comments_df = comments_df.filter(F.col("comment_id").isNotNull())

    cleaned_comments = comments_df.select(
        F.col("comment_id"),
        F.col("post_id"),
        F.col("parent_id"),
        F.trim(F.col("author")).alias("author"),
        _unescape_html(F.col("body")).alias("body"),
        F.col("score").cast("long").alias("score"),
        F.to_timestamp(F.col("created_utc")).alias("created_utc"),
    )

    cleaned_comments = cleaned_comments.dropDuplicates(["comment_id"])
    return cleaned_comments


def _validate_table_name(table_name: str) -> None:
    """Validate that table name follows safe naming conventions.

    Args:
        table_name: Name of the Delta table.

    Raises:
        ValueError: If table name contains invalid characters.
    """
    if not TABLE_NAME_PATTERN.match(table_name):
        raise ValueError(
            f"Invalid table name '{table_name}'. Must match pattern: "
            "start with letter/underscore, contain only alphanumeric/underscore/dot."
        )


def _merge_or_create_table(
    spark: SparkSession, df: DataFrame, table_name: str, merge_key: str
) -> int:
    """Create a new Delta table or merge data into an existing one.

    If the table does not exist, creates it with the DataFrame data.
    If the table exists, performs an upsert (MERGE) operation:
    - Rows with matching keys are updated with new values
    - New rows are inserted
    - Existing rows not in the source are retained

    Schema evolution is not allowed - mismatched schemas will cause an error.

    Args:
        spark: Active SparkSession.
        df: DataFrame containing the data to write.
        table_name: Name of the target Delta table.
        merge_key: Column name to use as the merge key (e.g., 'id', 'comment_id').

    Returns:
        Number of rows processed from the source DataFrame.

    Raises:
        AnalysisException: If schema mismatch occurs during merge.
    """
    _validate_table_name(table_name)

    if not spark.catalog.tableExists(table_name):
        logger.info(f"Creating new table {table_name}")
        (
            df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
        # Get row count from the newly created table
        row_count = spark.table(table_name).count()
        logger.info(f"Created table {table_name} with {row_count} rows")
    else:
        logger.info(f"Merging data into existing table {table_name}")
        # Use unique temp view name to avoid conflicts in concurrent scenarios
        temp_view_name = f"_source_data_{table_name.replace('.', '_')}"
        df.createOrReplaceTempView(temp_view_name)

        # Execute MERGE INTO statement
        merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING {temp_view_name} AS source
            ON target.{merge_key} = source.{merge_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        try:
            spark.sql(merge_sql)
            # Get row count after merge from source view (already materialized)
            row_count = spark.table(temp_view_name).count()
            logger.info(f"Merge completed for table {table_name} with {row_count} source rows")
        finally:
            # Clean up temp view
            spark.catalog.dropTempView(temp_view_name)
            logger.debug(f"Dropped temp view {temp_view_name}")

    return row_count


def _write_comments_table(spark: SparkSession, comments_df: DataFrame, table_name: str) -> int:
    """Write comments DataFrame to Delta table using upsert.

    Creates the table if it doesn't exist, or merges data into the existing table.
    Rows with matching comment_id are updated, new rows are inserted,
    and existing rows not in the source are retained.

    Args:
        spark: Active SparkSession.
        comments_df: DataFrame containing cleaned comments.
        table_name: Name of the target Delta table.

    Returns:
        Number of rows processed.
    """
    logger.info(f"Processing comments for table={table_name}")
    return _merge_or_create_table(spark, comments_df, table_name, "comment_id")


def _clean_reddit_dataframe(df: DataFrame) -> DataFrame:
    """Clean and normalize the raw Reddit posts DataFrame.

    Applies HTML unescaping, type casting, null handling, and deduplication.

    Args:
        df: Raw DataFrame from fixture file.

    Returns:
        Cleaned DataFrame matching SCHEMA.
    """

    def _trim_or_null(col_name: str) -> F.Column:
        return F.when(F.col(col_name).isNotNull(), F.trim(F.col(col_name))).otherwise(
            None
        )

    cleaned = df.select(
        F.col("id"),
        _unescape_html(F.col("title")).alias("title"),
        _unescape_html(F.col("selftext")).alias("selftext"),
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
        F.col("sentiment_score_transformer").cast("double").alias("sentiment_score_transformer"),
        _trim_or_null("sentiment_bucket_transformer").alias("sentiment_bucket_transformer"),
        _trim_or_null("device").alias("device"),
        _trim_or_null("issue_type").alias("issue_type"),
        _trim_or_null("post_summary").alias("post_summary"),
        F.col("comments").alias("comments"),
    )

    cleaned = cleaned.filter(F.col("id").isNotNull())
    cleaned = cleaned.dropDuplicates(["id"])
    return cleaned


def get_reddit_data(table_name: str) -> tuple[int, int]:
    """Load reddit data into Delta tables.

    Creates two tables:
    - {table_name}: Contains cleaned post data
    - {table_name}_comments: Contains flattened comments with parent references

    Args:
        table_name: Base name for the Delta tables.

    Returns:
        Tuple of (post_count, comment_count).

    Raises:
        ValueError: If table_name contains invalid characters.
    """
    _validate_table_name(table_name)
    comments_table_name = f"{table_name}_comments"
    _validate_table_name(comments_table_name)

    spark = SparkSession.builder.getOrCreate()
    df = _load_fixture_dataframe(spark)
    cleaned_df = _clean_reddit_dataframe(df)

    logger.info(f"Processing posts for table={table_name}")
    post_count = _merge_or_create_table(spark, cleaned_df, table_name, "id")

    comments_df = _extract_comments_dataframe(spark, cleaned_df)
    comment_count = _write_comments_table(spark, comments_df, comments_table_name)

    return post_count, comment_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Load reddit data into Delta table.")
    parser.add_argument("--table-name", default=None)
    args = parser.parse_args()
    if not args.table_name:
        raise ValueError("table_name is required. Pass --table-name.")
    post_count, comment_count = get_reddit_data(table_name=args.table_name)
    logger.info(
        f"Completed: {post_count} posts written to {args.table_name}, "
        f"{comment_count} comments written to {args.table_name}_comments"
    )
