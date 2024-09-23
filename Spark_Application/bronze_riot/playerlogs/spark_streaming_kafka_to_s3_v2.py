from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
    IntegerType,
)
from pyspark.sql.functions import sha2
import os
import sys

# spark-submit --master yarn --deploy-mode cluster --conf spark.kafka.bootstrap.servers="airflow-mysql-01:29092" --conf spark.kafka.topic="lol" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 ./spark_streaming_kafka_to_s3.py

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

"""
Spark 세션 설정
"""

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

"""
Kafka 설정
"""
kafka_bootstrap_server = sys.argv[1]
kafka_topic = sys.argv[2]

kafkaStream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_server)
    .option("subscribe", kafka_topic)
    .load()
)

"""
스키마 정의
원본 데이터 자체가 String 형이기 때문에 모든 필드를 String Type으로 정의
"""
schema = StructType(
    [
        StructField("createRoomDate", StringType(), False),
        StructField("method", StringType(), False),
        StructField("ingametime", StringType(), False),
        StructField("ip", StringType(), False),
        StructField("deathCount", StringType(), False),
        StructField("roomID", StringType(), False),
        StructField("datetime", StringType(), False),
        StructField("x", StringType(), False),
        StructField("y", StringType(), False),
        StructField("inputkey", StringType(), False),
        StructField("account", StringType(), False),
        StructField("champion", StringType(), False),
        StructField("status", StringType(), False),
    ]
)

"""
Json 파싱
"""

jsonParsedStream = (
    kafkaStream.selectExpr("CAST(value As STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)


"""
적합성 및 유효성 검사

해당 컬럼이 null값을 체크하고, 특정 컬럼은 음수를 가지지 않도록 함

통과한 데이터는 validStream, 통과하지 않는 데이터는 invalidStream
"""
validStream = jsonParsedStream.filter(
    (col("createRoomDate").isNotNull())
    & (col("ingametime").cast(IntegerType()).isNotNull())
    & (col("deathCount").cast(IntegerType()).isNotNull())
    & (col("deathCount").cast(IntegerType()) >= 0)
    & (col("x").cast(IntegerType()).isNotNull())
    & (col("y").cast(IntegerType()).isNotNull())
    & (col("status").cast(IntegerType()).isNotNull())
    & (col("status").cast(IntegerType()) >= 0)
)

invalidStream = jsonParsedStream.filter(
    (col("createRoomDate").isNull())
    | (col("ingametime").cast(IntegerType()).isNull())
    | (col("deathCount").cast(IntegerType()).isNull())
    | (col("deathCount").cast(IntegerType()) <= 0)
    | (col("x").cast(IntegerType()).isNull())
    | (col("y").cast(IntegerType()).isNull())
    | (col("status").cast(IntegerType()).isNull())
    | (col("status").cast(IntegerType()) <= 0)
)

transformedPlayerLogs = (
    validStream.select(
        col("createRoomDate").cast(DateType()).alias("create_room_date"),
        col("method"),
        col("ingametime").cast(IntegerType()).alias("current_time"),
        col("deathCount").cast(IntegerType()).alias("death_count"),
        col("roomID").alias("room_id"),
        col("datetime").cast(TimestampType()).alias("datetime"),
        col("x").cast(IntegerType()),
        col("y").cast(IntegerType()),
        col("inputkey"),
        col("account"),
        col("champion"),
        col("status").cast(IntegerType()),
    )
    .withColumn("encrypted_account", sha2(col("account"), 256))
    .drop("account")
    .drop("ip")
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

invalidStreamWriter = (
    invalidStream.writeStream.trigger(processingTime="1 minute")
    .outputMode("append")
    .format("json")
    .option("path", "s3://sjm-simple-data/invalid_data/")
    .option("checkpointLocation", "s3://sjm-simple-data/checkpoint/invalid_data/")
    .queryName("query_invalidData")
    .start()
)

playerLogsStreamWriter.awaitTermination()
invalidStreamWriter.awaitTermination()
