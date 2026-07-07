import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaConsumerTest") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .config("spark.sql.streaming.checkpointFileManagerClass", "org.apache.spark.sql.execution.streaming.FileContextBasedCheckpointFileManager") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "clickstream-events") \
    .option("startingOffsets", "latest") \
    .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/spark-checkpoints/kafka_consumer_test") \
    .start()

query.awaitTermination()