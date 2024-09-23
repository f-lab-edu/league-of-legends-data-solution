import os
import sys
from pyspark.sql import SparkSession
import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("Parquet-Backup")
    .master("yarn")
    .config("spark.home", "/usr/lib/spark")
    .config("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "AES256")
    .enableHiveSupport()
    .getOrCreate()
)

filter_time = sys.argv[1]

day = datetime.datetime.strptime(filter_time, "%Y-%m-%d")

partition_day = day.replace(day=1).strftime("%Y-%m")

base_path = "s3://sjm-simple-data/bronze_riot/playerlogs"

backup_bronze_data = spark.read.json(f"{base_path}/create_room_date={partition_day}-*")

backup_bronze_data.write.mode("overwrite").option("compression", "gzip").parquet(
    f"s3://sjm-backup/{partition_day}"
)

spark.stop()
