import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import first, last, col

spark = SparkSession.builder \
    .appName("SilverOrders") \
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
    CREATE TABLE IF NOT EXISTS local.silver.order_events (
        order_id STRING,
        user_id STRING,
        current_status STRING,
        amount DOUBLE,
        items ARRAY<STRING>,
        first_event_time TIMESTAMP,
        last_event_time TIMESTAMP
    ) USING iceberg
""")

bronze_df = spark.table("local.bronze.order_events")
print(f"Bronze row count: {bronze_df.count()}")

order_window = Window.partitionBy("order_id").orderBy("event_time") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

reconstructed_df = bronze_df \
    .withColumn("user_id_filled", first(col("user_id"), ignorenulls=True).over(order_window)) \
    .withColumn("amount_filled", first(col("amount"), ignorenulls=True).over(order_window)) \
    .withColumn("items_filled", first(col("items"), ignorenulls=True).over(order_window)) \
    .withColumn("current_status", last(col("status"), ignorenulls=True).over(order_window)) \
    .withColumn("first_event_time", first(col("event_time")).over(order_window)) \
    .withColumn("last_event_time", last(col("event_time")).over(order_window)) \
    .select(
        col("order_id"),
        col("user_id_filled").alias("user_id"),
        col("current_status"),
        col("amount_filled").alias("amount"),
        col("items_filled").alias("items"),
        col("first_event_time"),
        col("last_event_time")
    ) \
    .dropDuplicates(["order_id"])

print(f"Reconstructed order count: {reconstructed_df.count()}")

reconstructed_df.writeTo("local.silver.order_events").overwritePartitions()

print("Silver orders write complete")
spark.table("local.silver.order_events").show(20, truncate=False)