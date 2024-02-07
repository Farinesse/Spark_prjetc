from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def wordcount(df, col_name):
    return df.withColumn('word', F.explode(F.split(F.col(col_name), ' '))) \
        .groupBy('word') \
        .agg(F.count('word').alias('count'))

def main():
    # Initialiser la session Spark
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    
    input_path = "src/resources/exo1/data.csv"

    # Lire le fichier CSV
    df = spark.read.option("header", True).csv(input_path)

    df_count = wordcount(df, 'text')

    
    output_path = "data/exo1/output"
    df_count.write.partitionBy("count").mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    main()
