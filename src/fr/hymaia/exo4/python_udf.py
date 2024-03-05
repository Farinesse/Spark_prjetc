import time
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_category(category):
    category = int(category) 
    if category < 6:
        return "food"
    else:
        return "furniture"

categorize_category_udf = udf(categorize_category, StringType())

def main():
    spark = SparkSession.builder.appName("PythonUDF").master("local[*]").getOrCreate()

    df = spark.read.csv("./src/resources/exo4/sell.csv", header=True)
    df.show()
    df = df.withColumn("category_name", categorize_category_udf(df["category"]))  # Using UDF here
    
    start_time = time.time()

    df.write.csv("data/exo4/python", header=True, mode="overwrite")
    df.count()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Temps écoulé pour écrire le DataFrame:", elapsed_time, "secondes")
    spark.stop()

if __name__ == "__main__":
    main()
