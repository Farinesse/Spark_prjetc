from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.master("local[*]").appName("window_functions").getOrCreate()

    sell_df = spark.read.csv("/src/resources/exo4/sell.csv", header=True, inferSchema=True)

    sell_df = sell_df.withColumn("category_name", F.when(F.col("category") < 6, "food").otherwise("furniture"))

    window_spec_day_category = Window.partitionBy("date", "category_name")

    sell_df = sell_df.withColumn("total_price_per_category_per_day", F.sum("price").over(window_spec_day_category))

    window_spec_last_30_days = Window.partitionBy("category_name").orderBy("date").rowsBetween(-30, 0)

    sell_df = sell_df.withColumn("total_price_per_category_per_day_last_30_days", F.sum("price").over(window_spec_last_30_days))

    sell_df.show()

    spark.stop()