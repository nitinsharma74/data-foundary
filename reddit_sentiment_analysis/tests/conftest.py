import importlib.util
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

DELTA_AVAILABLE = importlib.util.find_spec("delta") is not None
SRC_MODULE = Path(__file__).resolve().parents[1] / "src" / "get_reddit_data.py"


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory) -> SparkSession:
    warehouse_dir = tmp_path_factory.mktemp("spark-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("reddit-data-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse_dir}")
    )
    if DELTA_AVAILABLE:
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    # Python workers (mapInPandas) unpickle functions by module name and do not
    # inherit pytest's pythonpath, so ship the module to them explicitly.
    session.sparkContext.addPyFile(str(SRC_MODULE))
    yield session
    session.stop()
