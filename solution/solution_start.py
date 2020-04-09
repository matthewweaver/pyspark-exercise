import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_spark_views(spark: SparkSession, customers_location: str, products_location: str,
                       transactions_location: str):
    spark.read.csv(customers_location, header=True).createOrReplaceTempView("customers")
    spark.read.csv(products_location, header=True).createOrReplaceTempView("products")
    spark.read.json(transactions_location).createOrReplaceTempView("raw_transactions")


def run_transformations(spark: SparkSession, customers_location: str, products_location: str,
                        transactions_location: str, output_location: str):
    create_spark_views(spark, customers_location, products_location, transactions_location)

    spark.sql("""SELECT raw_transactions.customer_id, raw_transactions.basket 
        FROM raw_transactions"""). \
        select("*", F.explode("basket").alias("exploded_data")). \
        select("customer_id", "exploded_data.product_id").\
        groupby("customer_id", "product_id").count().sort("customer_id").createOrReplaceTempView("transactions_grouped")

    output = spark.sql("""SELECT transactions_grouped.customer_id, customers.loyalty_score, 
    transactions_grouped.product_id, products.product_category,
    transactions_grouped.count 
    FROM transactions_grouped 
    LEFT JOIN customers ON transactions_grouped.customer_id = customers.customer_id
    LEFT JOIN products ON transactions_grouped.product_id = products.product_id""")

    #TODO: This will put all data on a single worker - might be worth investigating another solution e.g. FileUtil.CopyMerge
    output.repartition(1).write.csv("./output_data/outputs/")

    #TODO: Use logger
    print("Success!")
    #TODO: Add header to output
    #TODO: Check output
    #TODO: Add tests
    #TODO: Think of suggestions for how data scientists could use this data on a weekly basis

    # spark.sql("""SELECT customers.customer_id, customers.loyalty_score, raw_transactions.basket
    # FROM customers LEFT JOIN raw_transactions ON customers.customer_id = raw_transactions.customer_id""").\
    #     select("*", F.explode("basket").alias("exploded_data")).\
    #     select("customer_id", "loyalty_score", "exploded_data.product_id").\
    #     createOrReplaceTempView("transactions")
    # output = spark.sql("""SELECT transactions.customer_id, transactions.loyalty_score, transactions.product_id,
    # products.product_category FROM transactions LEFT JOIN products ON transactions.product_id = products.product_id""")
    # output_grouped = output.groupby("customer_id", "product_id").count().sort("customer_id")
    # output_joined = output.join(output_grouped, ["customer_id","product_id"]).createOrReplaceTempView("output")


def get_latest_transaction_date(spark: SparkSession):
    result = spark.sql("""SELECT MAX(date_of_purchase) AS date_of_purchase FROM raw_transactions""").collect()[0]
    max_date = result.date_of_purchase
    return max_date

#TODO: Do I need to preprocess the data e.g. dates?
def to_canonical_date_str(date_to_transform):
    return date_to_transform.strftime('%Y-%m-%d')


if __name__ == "__main__":
    spark_session = (
            SparkSession.builder
                        .master("local[2]")
                        .appName("DataTest")
                        .config("spark.executorEnv.PYTHONHASHSEED", "0")
                        .getOrCreate()
    )

    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    args = vars(parser.parse_args())

    run_transformations(spark_session, args['customers_location'], args['products_location'],
                        args['transactions_location'], args['output_location'])
