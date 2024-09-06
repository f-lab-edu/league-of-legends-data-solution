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


filter_time = spark.conf.get("spark.config.partition.time")

silver_playerlogs_df = spark.read.parquet(
    "s3://sjm-simple-data/silver_analysis_riot/"
).filter("create_room_date='" + filter_time + "'")


average_death_and_time = silver_playerlogs_df.groupBy(
    "encrypted_account", "champion", "create_room_date"
).agg(
    avg("death_count").alias("average_deaths"),
    avg("current_time").alias("average_game_time"),
)

room_end_time = silver_playerlogs_df.groupBy("room_id", "create_room_date").agg(
    max("current_time").alias("end_time")
)

key_used_per = (
    silver_playerlogs_df.groupBy("champion", "create_room_date")
    .agg(
        count(when(col("inputkey") == "alt", True)).alias("used"),
        count("*").alias("total"),
    )
    .withColumn("total_q", round(col("used") / col("total"), 2))
)

champion_death_count = (
    silver_playerlogs_df.groupBy("room_id", "champion", "create_room_date")
    .agg(max("death_count").alias("death_count"))
    .orderBy("room_id")
)

room_champion_xy = silver_playerlogs_df.groupBy(
    "room_id", "encrypted_account", "champion", "create_room_date"
).agg(stddev(col("x")), stddev(col("y")))


average_death_and_time.write.format("parquet").mode("append").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/average_death_and_time/")

room_end_time.write.format("parquet").mode("append").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/room_end_time/")
key_used_per.write.format("parquet").mode("append").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/key_used_per/")
champion_death_count.write.format("parquet").mode("append").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/champion_death_count/")
room_champion_xy.write.format("parquet").mode("append").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/gold_riot/room_champion_xy/")

spark.stop()
