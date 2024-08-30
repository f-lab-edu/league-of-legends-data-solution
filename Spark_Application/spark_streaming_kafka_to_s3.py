from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import sha2
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("League Of Legend Player Streaming") \
  .master("yarn") \
  .config("spark.jars.packages",
          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0") \
  .config("spark.home", "/usr/lib/spark") \
  .enableHiveSupport() \
  .getOrCreate()

kafkaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "<ip>:<port>") \
  .option("subscribe", "<topic>") \
  .load()

schema = StructType([
  StructField("createRoomDate", StringType(), True),
  StructField("method", StringType(), True),
  StructField("ingametime", StringType(), True),
  StructField("ip", StringType(), True),
  StructField("deathCount", StringType(), True),
  StructField("roomID", StringType(), True),
  StructField("datetime", StringType(), True),
  StructField("x", StringType(), True),
  StructField("y", StringType(), True),
  StructField("inputkey", StringType(), True),
  StructField("account", StringType(), True),
  StructField("champion", StringType(), True),
  StructField("status", StringType(), True)
])

jsonParsedStream = kafkaStream.selectExpr("CAST(value As STRING)") \
  .select(from_json(col("value"), schema).alias("data")) \
  .select("data.*")

jsonParsedStream = jsonParsedStream.withColumn("account_sha2", sha2(col("account"), 256))
jsonParsedStream = jsonParsedStream.drop("account")

transformedPlayerLogs = jsonParsedStream.toDF("createRoomDate", "method", "ingametime", "ip",
                           "deathCount", "roomID", "datetime", "x", "y",
                           "inputkey", "champion", "status", "account_sha2")

playerLogsStreamWriter = transformedPlayerLogs.writeStream \
  .trigger(processingTime='1 minute') \
  .outputMode("append") \
  .format("json") \
  .option("path", "s3://sjm-simple-data/bronzelayer/playerlogs/") \
  .option("checkpointLocation", "s3://sjm-simple-data/checkpoint/Riot/") \
  .queryName("query_playerLogs") \
  .start()

playerLogsStreamWriter.awaitTermination()
