from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def categorize_category(category):
    return "food" if category < 6 else "furniture"


categorize_category_udf = udf(categorize_category, StringType())


def main():
    spark = SparkSession.builder.master("local[*]").appName("python_udf").getOrCreate()

    sell_df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    sell_df = sell_df.withColumn("category_name", categorize_category_udf(col("category")))
    sell_df.show()

    spark.stop()