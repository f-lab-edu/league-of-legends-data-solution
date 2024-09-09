from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
)
from pyspark.sql.functions import sha2
import os
import sys

# spark-submit --master yarn --deploy-mode cluster --conf spark.kafka.bootstrap.servers="airflow-mysql-01:29092" --conf spark.kafka.topic="lol" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 ./spark_streaming_kafka_to_s3.py

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("League Of Legend Player Streaming")
    .master("yarn")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0",
    )
    .config("spark.home", "/usr/lib/spark")
    .enableHiveSupport()
    .getOrCreate()
)

kafka_bootstrap_server = spark.conf.get("spark.kafka.bootstrap.servers")
kafka_topic = spark.conf.get("spark.kafka.topic")

kafkaStream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_server)
    .option("subscribe", kafka_topic)
    .load()
)

schema = StructType(
    [
        StructField("createRoomDate", DateType(), True),
        StructField("method", StringType(), True),
        StructField("ingametime", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("deathCount", StringType(), True),
        StructField("roomID", StringType(), True),
        StructField("datetime", TimestampType(), True),
        StructField("x", StringType(), True),
        StructField("y", StringType(), True),
        StructField("inputkey", StringType(), True),
        StructField("account", StringType(), True),
        StructField("champion", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

jsonParsedStream = (
    kafkaStream.selectExpr("CAST(value As STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

jsonParsedStream = (
    jsonParsedStream.withColumn("create_room_date", col("createRoomDate"))
    .withColumn("current_time", col("ingametime"))
    .withColumn("room_id", col("roomID"))
    .withColumn("death_count", col("deathCount").cast("int"))
    .withColumn("x", col("x").cast("int"))
    .withColumn("y", col("y").cast("int"))
    .withColumn("status", col("status").cast("int"))
    .drop("createRoomDate")
    .drop("ingametime")
    .drop("deathCount")
    .drop("roomID")
)

jsonParsedStream = (
    jsonParsedStream.withColumn("encrypted_account", sha2(col("account"), 256))
    .drop("account")
    .drop("ip")
)

jsonParsedStream = jsonParsedStream.select(
    "create_room_date",
    "method",
    "current_time",
    "death_count",
    "room_id",
    "datetime",
    "encrypted_account",
    "x",
    "y",
    "inputkey",
    "champion",
    "status",
)

transformedPlayerLogs = jsonParsedStream.toDF(
    "create_room_date",
    "method",
    "current_time",
    "death_count",
    "room_id",
    "datetime",
    "encrypted_account",
    "x",
    "y",
    "inputkey",
    "champion",
    "status",
)


playerLogsStreamWriter = (
    transformedPlayerLogs.writeStream.trigger(processingTime="1 minute")
    .outputMode("append")
    .format("json")
    .option("path", "s3://sjm-simple-data/bronze_riot/playerlogs/")
    .option(
        "checkpointLocation", "s3://sjm-simple-data/checkpoint/bronze_riot/playerlogs/"
    )
    .partitionBy("create_room_date")
    .queryName("query_playerLogs")
    .start()
)

playerLogsStreamWriter.awaitTermination()
