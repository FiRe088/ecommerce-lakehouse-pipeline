import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SilverClickstream") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "C:/iceberg-warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.silver.clickstream_events (
        user_id STRING,
        session_id STRING,
        event_type STRING,
        page STRING,
        timestamp STRING,
        event_time TIMESTAMP
    ) USING iceberg
""")

bronze_df = spark.table("local.bronze.clickstream_events")
print(f"Bronze row count: {bronze_df.count()}")

deduped_df = bronze_df.dropDuplicates(["user_id", "session_id", "event_type", "page", "timestamp"])
print(f"Deduped row count: {deduped_df.count()}")

deduped_df.writeTo("local.silver.clickstream_events").overwritePartitions()

print("Silver clickstream write complete")
spark.table("local.silver.clickstream_events").show(20, truncate=False)