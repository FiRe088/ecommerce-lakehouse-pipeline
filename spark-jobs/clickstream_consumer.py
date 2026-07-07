import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("ClickstreamConsumer") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "C:/iceberg-warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.bronze.clickstream_events (
        user_id STRING,
        session_id STRING,
        event_type STRING,
        page STRING,
        timestamp STRING,
        event_time TIMESTAMP
    ) USING iceberg
""")

clickstream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("timestamp", StringType(), True)
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "clickstream-events") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), clickstream_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

def write_to_bronze(batch_df, batch_id):
    print(f"Writing batch {batch_id}, {batch_df.count()} rows")
    batch_df.writeTo("local.bronze.clickstream_events").append()

query = parsed_df.writeStream \
    .foreachBatch(write_to_bronze) \
    .option("checkpointLocation", "C:/spark-checkpoints/clickstream_bronze") \
    .start()

query.awaitTermination()