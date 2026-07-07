import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum, coalesce, lit, col, when

spark = SparkSession.builder \
    .appName("GoldUserSummary") \
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
    CREATE TABLE IF NOT EXISTS local.gold.user_summary (
        user_id STRING,
        total_clicks BIGINT,
        total_orders BIGINT,
        total_revenue DOUBLE,
        conversion_rate DOUBLE
    ) USING iceberg
""")

clicks_df = spark.table("local.silver.clickstream_events") \
    .groupBy("user_id") \
    .agg(count("*").alias("total_clicks"))

orders_df = spark.table("local.silver.order_events") \
    .groupBy("user_id") \
    .agg(
        count("*").alias("total_orders"),
        spark_sum("amount").alias("total_revenue")
    )

summary_df = clicks_df.join(orders_df, on="user_id", how="full_outer") \
    .withColumn("total_clicks", coalesce(col("total_clicks"), lit(0))) \
    .withColumn("total_orders", coalesce(col("total_orders"), lit(0))) \
    .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0.0))) \
    .withColumn(
        "conversion_rate",
        when(col("total_clicks") > 0, col("total_orders") / col("total_clicks")).otherwise(lit(0.0))
    )

print(f"User summary row count: {summary_df.count()}")

summary_df.writeTo("local.gold.user_summary").overwritePartitions()

print("Gold user summary write complete")
spark.table("local.gold.user_summary").orderBy(col("total_revenue").desc()).show(20, truncate=False)