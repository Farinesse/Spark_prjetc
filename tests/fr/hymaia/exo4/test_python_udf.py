from  src.fr.hymaia.exo4.python_udf import categorize_category 
from src.fr.hymaia.exo4.python_udf import categorize_category_udf
import unittest
from tests.fr.hymaia.spark_test_case import spark



class TestCategorizeCategory(unittest.TestCase):

  
# Tests unitaires pour la fonction categorize_category

    def test_food_category(self):
        self.assertEqual(categorize_category(4), "food")

    def test_furniture_category(self):
        self.assertEqual(categorize_category(7), "furniture")

    # Ajoutez d'autres cas de test selon les besoins


# Tests d'intégration pour l'UDF categorize_category_udf
class TestCategorizeCategoryUDF(unittest.TestCase):

    def setUp(self):
        # Initialisation de l'environnement Spark pour les tests d'intégration
        from pyspark.sql import SparkSession
        self.spark = SparkSession.builder \
            .appName("TestCategorizeCategoryUDF") \
            .getOrCreate()

    def tearDown(self):
        # Arrêt de l'environnement Spark après les tests d'intégration
        self.spark.stop()

    def test_udf(self):
        # Création d'un DataFrame Spark pour tester l'UDF
        df = self.spark.createDataFrame([(1,), (7,), (10,)], ["category"])
        df_result = df.withColumn("category_type", categorize_category_udf(df["category"])).collect()

        # Vérification des résultats
        expected_result = [("1", "food"), ("7", "furniture"), ("10", "furniture")]
        for row, expected in zip(df_result, expected_result):
            self.assertEqual((str(row["category"]), row["category_type"]), expected)


if __name__ == '__main__':
    unittest.main()