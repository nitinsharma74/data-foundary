import json
import pandas as pd
import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, LongType, TimestampType

from conftest import DELTA_AVAILABLE
from get_reddit_data import (
    COMMENTS_SCHEMA,
    SCHEMA,
    _clean_reddit_dataframe,
    _extract_comments_dataframe,
    _fixture_path,
    _flatten_comments,
    _load_fixture_dataframe,
    _merge_or_create_table,
    _validate_table_name,
)


def _count_nested(comments: list[dict]) -> int:
    return sum(1 + _count_nested(c.get("replies", [])) for c in comments)


@pytest.fixture(scope="session")
def fixture_pandas() -> pd.DataFrame:
    return pd.read_csv(_fixture_path())


@pytest.fixture(scope="session")
def cleaned_fixture_df(spark_session: SparkSession):
    return _clean_reddit_dataframe(_load_fixture_dataframe(spark_session)).cache()


def _post_row(post_id: str, comments: str | None) -> tuple:
    values = {field.name: None for field in SCHEMA.fields}
    values["id"] = post_id
    values["comments"] = comments
    return tuple(values[field.name] for field in SCHEMA.fields)


# --- existing behaviour -----------------------------------------------------


def test_clean_reddit_data_normalizes_fields(spark_session: SparkSession):
    raw_df = _load_fixture_dataframe(spark_session)
    cleaned_df = _clean_reddit_dataframe(raw_df)

    assert cleaned_df.filter(F.col("id").isNull()).count() == 0
    assert cleaned_df.select("id").distinct().count() == cleaned_df.count()
    assert isinstance(cleaned_df.schema["created_utc"].dataType, TimestampType)
    assert isinstance(cleaned_df.schema["ups"].dataType, LongType)
    assert isinstance(cleaned_df.schema["is_self"].dataType, BooleanType)


# --- fixture loading (regression for the multiLine/escape bug) ---------------


def test_load_fixture_row_count_matches_pandas(spark_session: SparkSession, fixture_pandas):
    raw_df = _load_fixture_dataframe(spark_session)

    assert len(fixture_pandas) == 34
    assert raw_df.count() == len(fixture_pandas)


def test_load_fixture_ids_are_unique_and_non_null(spark_session: SparkSession, fixture_pandas):
    raw_df = _load_fixture_dataframe(spark_session)
    ids = [row.id for row in raw_df.select("id").collect()]

    assert None not in ids
    assert len(ids) == len(set(ids))
    assert set(ids) == set(fixture_pandas["id"])


def test_load_fixture_comments_all_parse_as_json_lists(spark_session: SparkSession):
    raw_df = _load_fixture_dataframe(spark_session)
    comments = [row.comments for row in raw_df.select("comments").collect()]

    assert comments
    for value in comments:
        assert isinstance(json.loads(value), list)


def test_load_fixture_created_utc_parsed_as_timestamp(spark_session: SparkSession, fixture_pandas):
    raw_df = _load_fixture_dataframe(spark_session)
    # Format inside Spark (session TZ = UTC); collect() would shift to the driver's local TZ.
    by_id = {
        row.id: row.ts
        for row in raw_df.select(
            "id", F.date_format("created_utc", "yyyy-MM-dd'T'HH:mm:ss").alias("ts")
        ).collect()
    }

    first = fixture_pandas.iloc[0]
    expected = pd.Timestamp(first["created_utc"]).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S")
    assert raw_df.filter(F.col("created_utc").isNull()).count() == 0
    assert by_id[first["id"]] == expected


# --- _flatten_comments (pure Python) ----------------------------------------


def test_flatten_comments_chains_parent_ids_through_nesting():
    comments = [
        {
            "id": "c1",
            "author": "a",
            "body": "top",
            "score": 1,
            "created_utc": "2026-01-01T00:00:00+00:00",
            "replies": [
                {"id": "c2", "replies": [{"id": "c3", "replies": []}]},
            ],
        },
        {"id": "c4"},
    ]

    rows = list(_flatten_comments(comments, post_id="p1"))

    assert [(r["comment_id"], r["parent_id"]) for r in rows] == [
        ("c1", None),
        ("c2", "c1"),
        ("c3", "c2"),
        ("c4", None),
    ]
    assert {r["post_id"] for r in rows} == {"p1"}
    assert rows[0] == {
        "comment_id": "c1",
        "post_id": "p1",
        "parent_id": None,
        "author": "a",
        "body": "top",
        "score": 1,
        "created_utc": "2026-01-01T00:00:00+00:00",
    }


def test_flatten_comments_handles_deep_nesting():
    depth = 12
    node: dict = {"id": f"c{depth}"}
    for i in range(depth - 1, 0, -1):
        node = {"id": f"c{i}", "replies": [node]}

    rows = list(_flatten_comments([node], post_id="p1"))

    assert len(rows) == depth
    assert rows[-1]["comment_id"] == f"c{depth}"
    assert rows[-1]["parent_id"] == f"c{depth - 1}"


def test_flatten_comments_missing_fields_yield_none():
    rows = list(_flatten_comments([{}], post_id="p1"))

    assert rows == [
        {
            "comment_id": None,
            "post_id": "p1",
            "parent_id": None,
            "author": None,
            "body": None,
            "score": None,
            "created_utc": None,
        }
    ]


def test_flatten_comments_empty_list_yields_nothing():
    assert list(_flatten_comments([], post_id="p1")) == []


# --- _extract_comments_dataframe --------------------------------------------


def test_extract_comments_count_matches_fixture_json(
    spark_session: SparkSession, cleaned_fixture_df, fixture_pandas
):
    expected = sum(
        _count_nested(json.loads(value)) for value in fixture_pandas["comments"].dropna()
    )

    comments_df = _extract_comments_dataframe(spark_session, cleaned_fixture_df)

    assert expected > 0
    assert comments_df.count() == expected


def test_extract_comments_no_duplicate_or_null_comment_ids(
    spark_session: SparkSession, cleaned_fixture_df
):
    comments_df = _extract_comments_dataframe(spark_session, cleaned_fixture_df)

    assert comments_df.filter(F.col("comment_id").isNull()).count() == 0
    assert comments_df.select("comment_id").distinct().count() == comments_df.count()


def test_extract_comments_parents_and_posts_resolve(
    spark_session: SparkSession, cleaned_fixture_df
):
    comments_df = _extract_comments_dataframe(spark_session, cleaned_fixture_df)

    orphan_parents = comments_df.alias("c").join(
        comments_df.alias("p"), F.col("c.parent_id") == F.col("p.comment_id"), "left_anti"
    ).filter(F.col("c.parent_id").isNotNull())
    unknown_posts = comments_df.join(
        cleaned_fixture_df.select(F.col("id").alias("post_id")), "post_id", "left_anti"
    )

    assert orphan_parents.count() == 0
    assert unknown_posts.count() == 0


def test_extract_comments_output_schema_and_cleaning(spark_session: SparkSession):
    comments = json.dumps(
        [
            {
                "id": "c1",
                "author": "  bob  ",
                "body": " a &amp; b &lt;3 ",
                "score": 5,
                "created_utc": "2026-03-23T19:17:22+00:00",
                "replies": [{"id": "c2", "author": "amy", "body": "hi", "score": 1}],
            }
        ]
    )
    posts_df = spark_session.createDataFrame([_post_row("p1", comments)], schema=SCHEMA)

    comments_df = _extract_comments_dataframe(spark_session, posts_df)
    rows = {
        row.comment_id: row
        for row in comments_df.withColumn(
            "created_utc_str", F.date_format("created_utc", "yyyy-MM-dd HH:mm:ss")
        ).collect()
    }

    assert [f.name for f in comments_df.schema.fields] == [f.name for f in COMMENTS_SCHEMA.fields]
    assert isinstance(comments_df.schema["created_utc"].dataType, TimestampType)
    assert isinstance(comments_df.schema["score"].dataType, LongType)
    assert rows["c1"].author == "bob"
    assert rows["c1"].body == "a & b <3"
    assert rows["c1"].created_utc_str == "2026-03-23 19:17:22"
    assert rows["c2"].parent_id == "c1"
    assert rows["c2"].post_id == "p1"
    assert rows["c2"].created_utc is None


def test_extract_comments_empty_input_returns_empty_with_schema(spark_session: SparkSession):
    posts_df = spark_session.createDataFrame([], schema=SCHEMA)

    comments_df = _extract_comments_dataframe(spark_session, posts_df)

    assert comments_df.count() == 0
    assert comments_df.schema == COMMENTS_SCHEMA


def test_extract_comments_null_and_blank_comments_return_empty(spark_session: SparkSession):
    posts_df = spark_session.createDataFrame(
        [_post_row("p1", None), _post_row("p2", "   ")], schema=SCHEMA
    )

    comments_df = _extract_comments_dataframe(spark_session, posts_df)

    assert comments_df.count() == 0
    assert comments_df.schema == COMMENTS_SCHEMA


def test_extract_comments_skips_malformed_and_non_list_json(spark_session: SparkSession):
    posts_df = spark_session.createDataFrame(
        [
            _post_row("p1", "{not json"),
            _post_row("p2", json.dumps({"id": "x"})),
            _post_row("p3", json.dumps([{"id": "ok"}])),
            _post_row("p4", "[]"),
        ],
        schema=SCHEMA,
    )

    rows = _extract_comments_dataframe(spark_session, posts_df).collect()

    assert [(r.comment_id, r.post_id) for r in rows] == [("ok", "p3")]


def test_extract_comments_drops_comments_without_id(spark_session: SparkSession):
    posts_df = spark_session.createDataFrame(
        [_post_row("p1", json.dumps([{"body": "no id"}, {"id": "c1"}]))], schema=SCHEMA
    )

    rows = _extract_comments_dataframe(spark_session, posts_df).collect()

    assert [r.comment_id for r in rows] == ["c1"]


# --- _validate_table_name ---------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["workspace.default.dev_reddit_data", "workspace.default.prod_reddit_data_comments", "_t1"],
)
def test_validate_table_name_accepts_safe_names(name):
    _validate_table_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "",
        "1table",
        "workspace.default.t; DROP TABLE x",
        "t --comment",
        "t`",
        "t'",
        "a-b",
        "t/*x*/",
    ],
)
def test_validate_table_name_rejects_unsafe_names(name):
    with pytest.raises(ValueError, match="Invalid table name"):
        _validate_table_name(name)


def test_validate_table_name_rejects_trailing_newline():
    with pytest.raises(ValueError, match="Invalid table name"):
        _validate_table_name("workspace.default.t\n")


def test_merge_or_create_table_rejects_unsafe_name_before_touching_spark():
    class _ExplodingSpark:
        def __getattr__(self, item):
            raise AssertionError("spark should not be used")

    with pytest.raises(ValueError):
        _merge_or_create_table(_ExplodingSpark(), None, "t; DROP TABLE x", "id")


# --- _merge_or_create_table (Delta) -----------------------------------------


@pytest.mark.skipif(not DELTA_AVAILABLE, reason="delta-spark not installed")
def test_merge_or_create_table_create_then_idempotent_merge(spark_session: SparkSession):
    table = "default.reddit_merge_test"
    spark_session.sql(f"DROP TABLE IF EXISTS {table}")
    first = spark_session.createDataFrame([("c1", "a"), ("c2", "b")], "comment_id string, body string")

    assert _merge_or_create_table(spark_session, first, table, "comment_id") == 2
    assert _merge_or_create_table(spark_session, first, table, "comment_id") == 2

    rows = sorted(tuple(r) for r in spark_session.table(table).collect())
    assert rows == [("c1", "a"), ("c2", "b")]
    assert spark_session.sql(f"DESCRIBE DETAIL {table}").first().format == "delta"


@pytest.mark.skipif(not DELTA_AVAILABLE, reason="delta-spark not installed")
def test_merge_or_create_table_upserts_and_retains_rows(spark_session: SparkSession):
    table = "default.reddit_upsert_test"
    spark_session.sql(f"DROP TABLE IF EXISTS {table}")
    schema = "comment_id string, body string"
    _merge_or_create_table(
        spark_session,
        spark_session.createDataFrame([("c1", "old"), ("c2", "keep")], schema),
        table,
        "comment_id",
    )

    processed = _merge_or_create_table(
        spark_session,
        spark_session.createDataFrame([("c1", "new"), ("c3", "added")], schema),
        table,
        "comment_id",
    )

    rows = sorted(tuple(r) for r in spark_session.table(table).collect())
    assert processed == 2
    assert rows == [("c1", "new"), ("c2", "keep"), ("c3", "added")]
    temp_view = f"_source_data_{table.replace('.', '_')}"
    assert not spark_session.catalog.tableExists(temp_view)
