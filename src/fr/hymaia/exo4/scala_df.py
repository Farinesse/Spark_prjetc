from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("scala_udf").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()


def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    sell_df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    sell_df = sell_df.withColumn("category", addCategoryName(col("category")))
    sell_df.show()

    spark.stop()