import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, max, round, stddev
from pyspark.sql.types import DateType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("League Of Legend Player Batch Silver Layer")
    .master("yarn")
    .config("spark.home", "/usr/lib/spark")
    .enableHiveSupport()
    .getOrCreate()
)

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

champion_death_count.write.format("parquet").mode("overwrite").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/champion_death_count/")

spark.stop()
