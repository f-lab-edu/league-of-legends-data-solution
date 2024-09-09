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

key_used_per = (
    silver_playerlogs_df.groupBy("champion", "create_room_date")
    .agg(
        count(when(col("inputkey") == "alt", True)).alias("used"),
        count("*").alias("total"),
    )
    .withColumn("total_q", round(col("used") / col("total"), 2))
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

key_used_per.write.format("parquet").mode("overwrite").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/key_used_per/")

spark.stop()
