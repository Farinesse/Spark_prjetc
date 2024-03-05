import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import when

def main():
    spark = SparkSession.builder \
                        .appName("NoUDFExample") \
                        .master("local[2]") \
                        .getOrCreate()

    df = spark.read.csv("./src/resources/exo4/sell.csv", header=True)

    df = df.withColumn("category_name", when(df["category"] < 6, "food").otherwise("furniture"))
    
    start_time = time.time()

    df.write.csv("data/exo4/no_udf", header=True, mode="overwrite")
    # df.count()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Temps écoulé pour écrire le DataFrame:", elapsed_time, "secondes")



    df_formatted = df.withColumn("date", f.to_date("date"))

    df_total_price_per_category_per_day = calculate_total_price_per_category_per_day(df_formatted)
    
    df_total_price_per_category_per_day_last_30_days = calculate_total_price_per_category_per_day_last_30_days(df_formatted)


    df_total_price_per_category_per_day.write.csv("data/exo4/total_price_per_category_per_day", header=True, mode="overwrite")
    df_total_price_per_category_per_day_last_30_days.write.csv("data/exo4/total_price_per_category_per_day_last_30_days", header=True, mode="overwrite")
    
    spark.stop()

  
def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("category", "date")

    df = df.withColumn("total_price_per_category_per_day",
                       f.sum("price").over(window_spec))
    df = df.dropDuplicates(['date', "category_name", "total_price_per_category_per_day"])

    return df

def calculate_total_price_per_category_per_day_last_30_days(df):
    df = df.dropDuplicates(['date', "category_name"])
    window_spec = Window.partitionBy("category_name").orderBy("date").rowsBetween(-29, 0)

    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum("price").over(window_spec))

    return df.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days")

if __name__ == "__main__":
    main()
