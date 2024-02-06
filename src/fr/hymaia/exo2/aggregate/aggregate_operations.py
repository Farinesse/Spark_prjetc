from pyspark.sql.functions import count, desc, asc, col


def calculate_population_per_department(df):
    result_df = df.groupBy("department").agg(count("*").alias("nb_people"))
    
    result_df = result_df.na.drop(subset=["department"])
    
    result_df = result_df.orderBy(desc("nb_people"), asc("department")) 
    
    return result_df



def write_to_csv(df, output_path):
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
