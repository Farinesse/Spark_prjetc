from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.cleaning_operations import clean_data, join_and_select_columns, extract_department, write_to_parquet


def main(a1,a2,a3,spark):

    df_city = spark.read.option("header", True).option("delimiter", ',').csv(a1)
    df_clients = spark.read.option("header", True).option("delimiter", ',').csv(a2)
    

    df_city_cleaned = clean_data(df_clients)
    

    df_city_result = join_and_select_columns(df_city_cleaned, df_city)


  
    output_path = a3

    write_to_parquet(df_city_result, output_path)

    df_result_with_department = extract_department(df_city_result)


    