import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("ClickstreamConsumer") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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
    .select("data.*")

query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/spark-checkpoints/clickstream_consumer") \
    .start()

query.awaitTermination()