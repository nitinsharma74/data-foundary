from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, LongType, TimestampType

from get_reddit_data import _clean_reddit_dataframe, _load_fixture_dataframe


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("reddit-data-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def _fixture_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "fixtures" / "reddit_crunchyroll_45days.csv")


def test_clean_reddit_data_normalizes_fields(spark_session: SparkSession):
    raw_df = _load_fixture_dataframe(spark_session, _fixture_path())
    cleaned_df = _clean_reddit_dataframe(raw_df)

    assert cleaned_df.filter(F.col("id").isNull()).count() == 0
    assert cleaned_df.select("id").distinct().count() == cleaned_df.count()
    assert isinstance(cleaned_df.schema["created_utc"].dataType, TimestampType)
    assert isinstance(cleaned_df.schema["ups"].dataType, LongType)
    assert isinstance(cleaned_df.schema["is_self"].dataType, BooleanType)
