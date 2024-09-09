import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, isnull
from pyspark.ml.feature import StringIndexer
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

bronze_playerlogs_json = spark.sql(
    f"""
    SELECT * FROM bronze_riot.playerlogs WHERE create_room_date='{filter_time}'
"""
)

chamion_indexer = StringIndexer(
    inputCols=["champion", "room_id", "inputkey"],
    outputCols=["cahmpion_indexed", "room_indexed", "inputkey_indexed"],
)

chamion_indexer_model = chamion_indexer.fit(bronze_playerlogs_json)
final_data_with_transform = chamion_indexer_model.transform(bronze_playerlogs_json)

label_add_playlogs = final_data_with_transform.withColumn(
    "label", when(col("champion") == "VIKTOR", 1).otherwise(0)
).withColumn("create_room_date", col("datetime").cast(DateType()))

train_data = label_add_playlogs.withColumn("x+y", col("x") + col("y")).select(
    "cahmpion_indexed",
    "datetime",
    "encrypted_account",
    "inputkey_indexed",
    "x",
    "y",
    "label",
    "create_room_date",
)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

train_data.write.format("parquet").mode("overwrite").partitionBy("create_room_date").save(
    "s3://sjm-simple-data/silver_train_riot/playerlogs"
)

spark.stop()
