import sys
from unittest.mock import MagicMock

import pandas as pd

_spark_session = MagicMock()
_spark_session.builder = MagicMock()
sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock(SparkSession=_spark_session))

from src import deezer_top_tracks_ingestor


def test_fetch_top_tracks_parses_rows(monkeypatch):
    response = MagicMock()
    response.json.return_value = {
        "data": [
            {
                "id": 1,
                "title": "Track A",
                "duration": 123,
                "explicit_lyrics": False,
                "artist": {"id": 10, "name": "Artist A"},
                "album": {"id": 100, "title": "Album A"},
            }
        ]
    }
    response.raise_for_status.return_value = None
    monkeypatch.setattr(
        deezer_top_tracks_ingestor.requests, "get", lambda *args, **kwargs: response
    )
    monkeypatch.setattr(
        deezer_top_tracks_ingestor.SparkSession.builder,
        "getOrCreate",
        lambda: MagicMock(),
    )

    app = deezer_top_tracks_ingestor.DeezerTopTracksIngestor()
    df = app.fetch_top_tracks()

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "title_id"] == 1
    assert df.loc[0, "artist_name"] == "Artist A"


def test_write_table_inserts_when_missing(monkeypatch):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    spark_df = MagicMock()
    spark.createDataFrame.return_value = spark_df
    monkeypatch.setattr(
        deezer_top_tracks_ingestor.SparkSession.builder, "getOrCreate", lambda: spark
    )

    app = deezer_top_tracks_ingestor.DeezerTopTracksIngestor()
    app.write_table(pd.DataFrame([{"title_id": 1}]))

    spark_df.write.format.assert_called_with("delta")
    spark_df.write.format.return_value.mode.assert_called_with("overwrite")
