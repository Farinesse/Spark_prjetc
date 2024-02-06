from src.fr.hymaia.exo2.clean.cleaning_operations import clean_data, join_and_select_columns, extract_department
from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row


class TestCleaningOperations(unittest.TestCase):

    def test_clean_data(self):
        # GIVEN
        input_data = [
            Row(name='Cussac', age=27, zip='75020'),
            Row(name='Farines', age=17, zip='75021')
        ]
        input_df = spark.createDataFrame(input_data)

        # WHEN
        cleaned_df = clean_data(input_df)

        # THEN
        expected_data = [
            Row(name='Cussac', age=27, zip='75020')
            ]
        expected_df = spark.createDataFrame(expected_data)
        self.assertEqual(cleaned_df.collect(), expected_df.collect())
    

    def test_join_and_select_columns_column_names(self):
        # GIVEN
        clients_data = [
            Row(name='Cussac', age=27, zip='75020'),
        ]
        villes_data = [
            Row(zip='75020', city='Paris'),  
        ]
        df_clients = spark.createDataFrame(clients_data)
        df_villes = spark.createDataFrame(villes_data)

        # WHEN
        result_df = join_and_select_columns(df_clients, df_villes)
        result_df.show()

        # THEN
        expected_columns = ['name', 'age', 'zip', 'city'] 
        self.assertEqual(result_df.columns, expected_columns)

   
    def test_join_and_select_columns_no_match(self):
        # GIVEN
        clients_data = [
            Row(name='Cussac', age=27, zip='75020'),
        ]
        villes_data = [
            Row(zip='75019', city='Paris'), 
        ]
        df_clients = spark.createDataFrame(clients_data)
        df_villes = spark.createDataFrame(villes_data)

        # WHEN
        result_df = join_and_select_columns(df_clients, df_villes)
        result_df.show()

        # THEN
        self.assertEqual(result_df.count(), 1)  # On s'attend à ce qu'il n'y ait aucune ligne dans le DataFrame de sortie



    def test_join_and_select_columns(self):
        # GIVEN
        clients_data = [
            Row(name='Cussac', age=27, zip='75020'),
        ]
        villes_data = [
            Row(zip='75020', city='Paris'),
        ]
        df_clients = spark.createDataFrame(clients_data)
        df_villes = spark.createDataFrame(villes_data)

        # WHEN
        result_df = join_and_select_columns(df_clients, df_villes)

        # THEN
        expected_data = [
            Row(name='Cussac', age=27, zip='75020', city='Paris'),
        ]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result_df.collect(), expected_df.collect())

    
    
    def test_extract_department(self):
        # GIVEN
        result_data = [
            Row(name='Cussac', age=27, zip='75020', city='Paris'),
            Row(name='Corsican', age=30, zip='20180', city='Ajaccio'),  
            Row(name='OtherCorsican', age=35, zip='20500', city='Porto-Vecchio'), 
            Row(name='BeyondCorsica', age=40, zip='21000', city='Dijon'),  
        ]
        df_result = spark.createDataFrame(result_data)

        # WHEN
        result_df = extract_department(df_result)

        # THEN
        expected_data = [
            Row(name='Cussac', age=27, zip='75020', city='Paris', department='75'),
            Row(name='Corsican', age=30, zip='20180', city='Ajaccio', department='2A'),
            Row(name='OtherCorsican', age=35, zip='20500', city='Porto-Vecchio', department='2B'),
            Row(name='BeyondCorsica', age=40, zip='21000', city='Dijon', department='21'),
        ]
        expected_df = spark.createDataFrame(expected_data)

        self.assertEqual(result_df.collect(), expected_df.collect())

def test_extract_department_with_null_department():
    # GIVEN
    result_data = [
        Row(name='Cussac', age=27, zip='75020', city='Paris'),
        Row(name='Corsican', age=30, zip='20180', city='Ajaccio'),  
        Row(name='OtherCorsican', age=35, zip='20500', city='Porto-Vecchio'), 
        Row(name='BeyondCorsica', age=40, zip='21000', city='Dijon'),  
        Row(name='NoCity', age=25, zip='10000', city=None), 
    ]
    df_result = spark.createDataFrame(result_data)

    # WHEN
    result_df = extract_department(df_result)

    # THEN
    expected_data = [
        Row(name='Cussac', age=27, zip='75020', city='Paris', department='75'),
        Row(name='Corsican', age=30, zip='20180', city='Ajaccio', department='2A'),
        Row(name='OtherCorsican', age=35, zip='20500', city='Porto-Vecchio', department='2B'),
        Row(name='BeyondCorsica', age=40, zip='21000', city='Dijon', department='21'),
        Row(name='NoCity', age=25, zip='10000', city=None, department=None),  
    ]
    expected_df = spark.createDataFrame(expected_data)

    assert result_df.collect() == expected_df.collect()
