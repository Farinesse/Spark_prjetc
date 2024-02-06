from src.fr.hymaia.exo2.aggregate.aggregate_operations import calculate_population_per_department
from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row

class TestCleaningOperations(unittest.TestCase): 
    def test_calculate_population_per_department(self):
       # GIVEN
        result_data = [
            Row(name='Cussac', age=27, zip='75020', city='Paris', department='75'),
            Row(name='Corsican', age=30, zip='20180', city='Ajaccio', department='2A'),
            Row(name='OtherCorsican', age=35, zip='75008', city='Porto-Vecchio', department='75'),
            Row(name='BeyondCorsica', age=40, zip='21000', city='Dijon', department='21'),
        ]
        df_result = spark.createDataFrame(result_data)

        # WHEN
       
        result_df = calculate_population_per_department(df_result)

        # THEN 
        expected_data = [
            Row(department='75', nb_people=2),
            Row(department='21', nb_people=1),
            Row(department='2A', nb_people=1),
        ]
        expected_df = spark.createDataFrame(expected_data)

        # Comparer les DataFrames
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_calculate_population_per_department_null_values(self):
        # GIVEN
        result_data = [
            Row(name='Cussac', age=27, zip='75020', city='Paris', department='75'),
            Row(name='Corsican', age=30, zip='20180', city='Ajaccio', department='2A'),
            Row(name='OtherCorsican', age=35, zip='75008', city=None, department=None),
            Row(name='BeyondCorsica', age=40, zip='21000', city='Dijon', department='21'),
        ]
        df_result = spark.createDataFrame(result_data)

        # WHEN
        result_df = calculate_population_per_department(df_result)

        # THEN 
        expected_data = [
            Row(department='21', nb_people=1),
            Row(department='2A', nb_people=1),
            Row(department='75', nb_people=1),

        ]
        expected_df = spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())

    