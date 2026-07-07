import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkBootTest") \
    .master("local[1]") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")

print("Spark version:", spark.version)
df = spark.createDataFrame([(1, "test"), (2, "boot")], ["id", "value"])
df.show()
spark.stop()