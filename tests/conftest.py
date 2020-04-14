import shutil
from tempfile import mkdtemp

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
                    .master("local[2]")
                    .appName("DataTest")
                    .config("spark.executorEnv.PYTHONHASHSEED", "0")
                    .getOrCreate()
    )
    return spark

