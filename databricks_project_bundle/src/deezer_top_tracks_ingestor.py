from datetime import date

import pandas as pd
import requests
from pyspark.sql import SparkSession

try:
    from src.constants.deezer_constants import (
        DEEZER_TABLE_NAME,
        DEEZER_TOP_TRACKS_URL,
        REQUEST_TIMEOUT_SECONDS,
    )
except ModuleNotFoundError:
    from constants.deezer_constants import (
        DEEZER_TABLE_NAME,
        DEEZER_TOP_TRACKS_URL,
        REQUEST_TIMEOUT_SECONDS,
    )


class DeezerTopTracksIngestor:
    def __init__(
        self,
        url: str = DEEZER_TOP_TRACKS_URL,
        table_name: str = DEEZER_TABLE_NAME,
    ) -> None:
        self.url = url
        self.table_name = table_name
        self.spark = SparkSession.builder.getOrCreate()

    def fetch_top_tracks(self) -> pd.DataFrame:
        resp = requests.get(self.url, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()

        load_date = date.today().isoformat()
        rows = []
        for i, track in enumerate(data.get("data", []), start=1):
            rows.append(
                {
                    "title_id": track.get("id"),
                    "title": track.get("title"),
                    "duration": track.get("duration"),
                    "explicit_lyrics": track.get("explicit_lyrics"),
                    "position": i,
                    "load_date": load_date,
                    "artist_id": track.get("artist", {}).get("id"),
                    "artist_name": track.get("artist", {}).get("name"),
                    "album_id": track.get("album", {}).get("id"),
                    "album_name": track.get("album", {}).get("title"),
                }
            )

        return pd.DataFrame(rows)

    def write_table(self, df: pd.DataFrame) -> None:
        spark_df = self.spark.createDataFrame(df)

        if self.spark.catalog.tableExists(self.table_name):
            load_date = date.today().isoformat()
            self.spark.sql(
                f"DELETE FROM {self.table_name} WHERE load_date = '{load_date}'"
            )
            spark_df.write.format("delta").mode("append").saveAsTable(self.table_name)
        else:
            spark_df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)

    def run(self) -> None:
        df = self.fetch_top_tracks()
        self.write_table(df)


if __name__ == "__main__":
    DeezerTopTracksIngestor().run()
