from pyspark.sql import SparkSession
from .aggregate.aggregate_operations import calculate_population_per_department, write_to_csv
from .clean.cleaning_operations import clean_data, join_and_select_columns, extract_department, write_to_parquet

def main():
    spark = SparkSession.builder.appName("SparkJob").getOrCreate()

    df_cleaned = spark.read.parquet("data/exo2/output")

    df_result_with_department = extract_department(df_cleaned)
    
    df_aggregated = calculate_population_per_department(df_result_with_department)
    
    write_to_csv(df_aggregated, "data/exo2/aggregate")


if __name__ == "__main__":
    main()
