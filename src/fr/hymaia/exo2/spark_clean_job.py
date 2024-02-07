from pyspark.sql import SparkSession
from .clean.cleaning_operations import clean_data, join_and_select_columns, extract_department, write_to_parquet


def main():
    spark = SparkSession.builder.appName("clean").master("local[*]").getOrCreate()

    df_city = spark.read.option("header", True).option("delimiter", ',').csv("./src/resources/exo2/city_zipcode.csv")
    df_clients = spark.read.option("header", True).option("delimiter", ',').csv("./src/resources/exo2/clients_bdd.csv")
    
    df_city_cleaned = clean_data(df_clients)
    
    df_city_result = join_and_select_columns(df_city_cleaned, df_city)

  

    output_path = "./data/exo2/output"
    write_to_parquet(df_city_result, output_path)

    df_result_with_department = extract_department(df_city_result)

    