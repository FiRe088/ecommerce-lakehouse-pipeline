import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# NOTE: aws_sdk_jar path is specific to the Airflow worker container's Ivy cache location.
# This script is designed to run inside the Airflow Docker container (airflow-airflow-worker-1),
# not directly on Windows. Requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment
# variables to be set in that shell before running.
aws_sdk_jar = "/home/airflow/.ivy2/cache/com.amazonaws/aws-java-sdk-bundle/jars/aws-java-sdk-bundle-1.12.262.jar"

spark = SparkSession.builder \
    .appName("RebuildClickstreamS3") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.driver.extraClassPath", aws_sdk_jar) \
    .config("spark.executor.extraClassPath", aws_sdk_jar) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://ecommerce-lakehouse-iceberg-warehouse-fire088/warehouse") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("DROP TABLE IF EXISTS bronze.clickstream_events")
spark.sql("""
    CREATE TABLE bronze.clickstream_events (
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

raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "clickstream-events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), clickstream_schema).alias("data")) \
    .select("data.*") \
    .filter(col("user_id").isNotNull()) \
    .withColumn("event_time", to_timestamp(col("timestamp")))

parsed_df.writeTo("bronze.clickstream_events").append()

count = spark.table("bronze.clickstream_events").count()
print(f"Rebuilt bronze.clickstream_events on S3 with {count} rows")