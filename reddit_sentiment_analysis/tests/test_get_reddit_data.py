import pandas as pd

from get_reddit_data import _clean_reddit_dataframe, _load_fixture_dataframe


def test_clean_reddit_data_normalizes_fields():
    raw_df = _load_fixture_dataframe()
    cleaned_df = _clean_reddit_dataframe(raw_df)

    assert cleaned_df["id"].isna().sum() == 0
    assert cleaned_df["id"].is_unique
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df["created_utc"])
    assert pd.api.types.is_integer_dtype(cleaned_df["ups"])
