import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, max, round, stddev
from pyspark.sql.types import DateType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("LoL-SparkBatch-Delta-Gold-champion_death_count")
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
    .config("spark.sql.lineage.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

filter_time = sys.argv[1]

silver_playerlogs_df = spark.sql(
    f"""
    SELECT * FROM silver_analysis_riot.playerlogs WHERE create_room_date='{filter_time}'
    """
)

champion_death_count = (
    silver_playerlogs_df.groupBy("room_id", "champion", "create_room_date")
    .agg(max("death_count").alias("death_count"))
    .orderBy("room_id")
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

champion_death_count.write.format("delta").mode("overwrite").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/champion_death_count/")

spark.stop()
