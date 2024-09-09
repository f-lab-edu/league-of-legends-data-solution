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

room_end_time = silver_playerlogs_df.groupBy("room_id", "create_room_date").agg(
    max("current_time").alias("end_time")
)

room_champion_xy = silver_playerlogs_df.groupBy(
    "room_id", "encrypted_account", "champion", "create_room_date"
).agg(stddev(col("x")), stddev(col("y")))

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

room_end_time.write.format("parquet").mode("overwrite").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/room_end_time/")

spark.stop()
