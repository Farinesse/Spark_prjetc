from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import SparkSession

import subprocess
import os

def testintegration():
    job1_path = "src/fr/hymaia/exo2/spark_clean_job.py"

    subprocess.run(["spark-submit", job1_path])
    output_path = "data/exo2/clean.parquet"
    assert os.path.exists(output_path),"Le fichier parquet de sortie du job 2 n'a pas été créé."

    job2_path = "src/fr/hymaia/exo2/spark_aggregate_job.py"

    subprocess.run(["spark-submit", job2_path])
    
    output_path = "data/exo2/aggregate"
    assert os.path.exists(output_path), "Le fichier CSV de sortie du job 2 n'a pas été créé."

if __name__ == "__main__":
    test_integration() 