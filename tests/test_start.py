from datetime import datetime

import tempfile
import pytest
from pyspark.sql.context import SQLContext
from pyspark import Row

from solution.solution_start import \
    get_latest_transaction_date, create_spark_views, group_transactions, run_transformations

customers_location = "../input_data/starter/customers.csv"
products_location = "../input_data/starter/products.csv"
transactions_location = "../input_data/starter/transactions"

#TODO: Further work - rather than compare schema of dataframes, can mock the input data and compare dataframe


@pytest.mark.usefixtures("spark")
def test_get_latest_transaction_date_returns_most_recent_date(spark):
    spark.createDataFrame([
        Row(date_of_purchase=datetime(2018, 12, 1, 4, 15, 0)),
        Row(date_of_purchase=datetime(2019, 3, 1, 14, 10, 0)),
        Row(date_of_purchase=datetime(2019, 2, 1, 14, 9, 59)),
        Row(date_of_purchase=datetime(2019, 1, 2, 19, 14, 20))
    ]).createOrReplaceTempView("raw_transactions")

    expected = datetime(2019, 3, 1, 14, 10, 0)
    actual = get_latest_transaction_date(spark)

    assert actual == expected

@pytest.mark.usefixtures("spark")
def test_create_spark_views(spark):
    sql = SQLContext(spark)
    create_spark_views(spark, customers_location, products_location, transactions_location)
    assert sql.tableNames() == ['customers', 'products', 'raw_transactions']

@pytest.mark.usefixtures("spark")
def test_group_transactions(spark):
    sql = SQLContext(spark)
    create_spark_views(spark, customers_location, products_location, transactions_location)
    group_transactions(spark)
    columns = spark.sql("""SELECT * FROM transactions_grouped""").columns
    assert "transactions_grouped" in sql.tableNames()
    assert columns == ["customer_id", "product_id", "count"]

@pytest.mark.usefixtures("spark")
def test_run_transformations(spark):
    with tempfile.TemporaryDirectory() as output_location:
        run_transformations(spark, customers_location, products_location, transactions_location, output_location)
        columns = spark.read.csv(output_location, header="true").columns
        assert columns == ["customer_id", "loyalty_score", "product_id", "product_category", "purchase_count"]
