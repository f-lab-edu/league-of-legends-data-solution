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

bronze_playlogs_json = spark.read.json("s3://sjm-simple-data/bronze_riot/playerlogs/*")

transformed_playerlogs = bronze_playlogs_json.withColumn(
    "current_time", col("current_time").cast(IntegerType())
).withColumn("datetime", col("datetime").cast(TimestampType()))

# transformed_playerlogs.select(
#    [count(when(isnull(df1), df1)).alias(df1) for df1 in transformed_playerlogs.columns]
# ).show()

filtered_playlogs_nullcheck = (
    transformed_playerlogs.filter(
        ~(
            (transformed_playerlogs["champion"] == "")
            | (transformed_playerlogs["champion"].isNull())
            | isnan(bronze_playlogs_json["champion"])
        )
    )
    .filter(
        ~(
            (transformed_playerlogs["encrypted_account"] == "")
            | (transformed_playerlogs["encrypted_account"].isNull())
            | isnan(bronze_playlogs_json["encrypted_account"])
        )
    )
    .filter(
        ~(
            (transformed_playerlogs["room_id"] == "")
            | (transformed_playerlogs["room_id"].isNull())
            | isnan(bronze_playlogs_json["room_id"])
        )
    )
    .filter(~(isnan(bronze_playlogs_json["current_time"])))
    .filter(~(isnan(bronze_playlogs_json["datetime"])))
)

# filtered_playlogs_nullcheck.select(
#    [count(when(isnull(df1), df1)).alias(df1) for df1 in transformed_playerlogs.columns]
# ).show()

analysis_data = (
    filtered_playlogs_nullcheck.withColumn("x+y", col("x") + col("y"))
    .withColumn("buy", when(col("method") == "/buyItem", 1).otherwise(0))
    .orderBy("room_id", "current_time")
)

analysis_data = analysis_data.withColumn(
    "create_room_date", col("datetime").cast(DateType())
)

analysis_data.write.format("parquet").mode("append").partitionBy(
    "create_room_date"
).save("s3://sjm-simple-data/silver_analysis_riot/playerlogs")

spark.stop()
