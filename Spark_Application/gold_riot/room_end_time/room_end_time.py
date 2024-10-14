import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, max, round, stddev
from pyspark.sql.types import DateType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("LoL-SparkBatch-Delta-Gold-room_end_time")
    .master("yarn")
    .config("spark.home", "/usr/lib/spark")
    .config(
        "spark.jars",
        "/usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar,/usr/share/aws/delta/lib/delta-storage-s3-dynamodb.jar",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.driver.memory", "2G")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "2G")
    .config("spark.executor.instances", "1")
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

filter_time = sys.argv[1]

silver_playerlogs_df = spark.table("silver_analysis_riot.analysis_playerlogs").where(
    f"create_room_date='{filter_time}'"
)

room_end_time = silver_playerlogs_df.groupBy("room_id", "create_room_date").agg(
    max("current_time").alias("end_time")
)

room_champion_xy = silver_playerlogs_df.groupBy(
    "room_id", "encrypted_account", "champion", "create_room_date"
).agg(stddev(col("x")), stddev(col("y")))

# room_end_time.write.format("delta").mode("overwrite").partitionBy(
#    "create_room_date"
# ).save("s3://sjm-simple-data/gold_riot/room_end_time/")

room_end_time.write.format("delta").mode("overwrite").partitionBy(
    "create_room_date"
).saveAsTable("gold_riot.room_end_time")

spark.stop()
