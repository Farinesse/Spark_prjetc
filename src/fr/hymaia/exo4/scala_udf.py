import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_java_column, _to_seq


def addCategoryName(col):
    
    sc = spark.sparkContext
    
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    spark = SparkSession.builder.master("local[*]").appName("scala_udf").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()

    df = spark.read.csv("./src/resources/exo4/sell.csv", header=True)
    
    df = df.withColumn("category_name", addCategoryName(col("category")))
    
    start_time = time.time()
    
    #df.show()
    
    #df.count() 
    
    df.write.csv("data/exo4/scala", header=True, mode="overwrite")
    
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    
    print("Temps écoulé pour écrire le DataFrame:", elapsed_time, "secondes")
    
    spark.stop()



if __name__ == "__main__":
    main()
