import os
import pytest


pytest.importorskip("pyspark", reason="pyspark required for dc43 tests")
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("dc43-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(f"Spark not available (install Java/JDK and set JAVA_HOME). Details: {e}")
    else:
        yield spark
        spark.stop()
