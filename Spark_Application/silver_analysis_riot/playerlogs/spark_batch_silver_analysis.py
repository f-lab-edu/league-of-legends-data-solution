import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, isnull
from pyspark.sql.types import IntegerType, TimestampType, DateType

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

bronze_playerlogs_json = spark.sql(
    f"""
    SELECT * FROM bronze_riot.playerlogs WHERE create_room_date='{filter_time}'
"""
)

transformed_playerlogs = bronze_playerlogs_json.withColumn(
    "datetime", col("datetime").cast(TimestampType())
)

filtered_playlogs_nullcheck = transformed_playerlogs.filter(
    ~((col("champion") == "") | col("champion").isNull())
    | ~((col("encrypted_account") == "") | col("encrypted_account").isNull())
    | ~((col("room_id") == "") | col("room_id").isNull())
    | ~((col("current_time") == "") | col("current_time").isNull())
    | ~((col("datetime") == "") | col("datetime").isNull())
)

analysis_data = (
    filtered_playlogs_nullcheck.withColumn("x+y", col("x") + col("y"))
    .withColumn("buy", when(col("method") == "/buyItem", 1).otherwise(0))
    .orderBy("room_id", "current_time")
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

analysis_data.write.format("parquet").mode("overwrite").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/silver_analysis_riot/playerlogs")
spark.stop()
