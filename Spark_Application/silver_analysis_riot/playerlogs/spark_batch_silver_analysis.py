import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, isnull
from pyspark.sql.types import IntegerType, TimestampType, DateType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("LoL-SparkBatch-Delta-Silver-Analysis")
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
    .config("spark.executor.memory", "4G")
    .config("spark.executor.instances", "1")
    .config("spark.sql.shuffle.partitions", "4")
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

filter_time = sys.argv[1]

bronze_playerlogs_json = spark.table("bronze_riot.playerlogs").filter(
    col("create_room_date") == filter_time
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


# analysis_data.write.format("delta").mode("overwrite").partitionBy(
#     "create_room_date"
# ).save("s3://sjm-simple-data/silver_analysis_riot/playerlogs/")

analysis_data.write.format("delta").mode("overwrite").partitionBy(
    "create_room_date"
).saveAsTable("silver_analysis_riot.analysis_playerlogs")


print("Data processing complete")
spark.stop()
