from pyspark.sql.functions import col, substring, when
from pyspark.sql import functions as F


def clean_data(df_clients):
    # Filter clients with age >= 18
    df_cleaned = df_clients.filter(col("age") >= 18)
    return df_cleaned


def join_and_select_columns(df_cleaned, df_city):
    
    df_result = df_cleaned.join(df_city, "zip", "left")

    
    df_result = df_result.select("name", "age", "zip", "city")

    return df_result



def extract_department(df_result):
    df_result = df_result.withColumn("department", df_result["zip"].substr(1, 2))

    df_result = df_result.withColumn("department",
                                     F.when((df_result["city"] != "") & (df_result["city"].isNotNull()),
                                            F.when((df_result["zip"] <= "20190") & (df_result["zip"] >= "20000"), "2A")
                                            .when((df_result["zip"] > "20190") & (df_result["zip"] < "21000"), "2B")
                                            .otherwise(df_result["department"].substr(0, 2)))
                                     .otherwise(None))  
 
    return df_result


def write_to_parquet(df, output_path):

    df.write.parquet(output_path, mode="overwrite")