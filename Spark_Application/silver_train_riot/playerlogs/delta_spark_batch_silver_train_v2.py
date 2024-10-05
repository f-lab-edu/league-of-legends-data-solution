import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt, lag, pow, round, to_date
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("LoL-SparkBatch-Delta-Silver-Train")
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
    .enableHiveSupport()
    .getOrCreate()
)

filter_time = sys.argv[1]

bronze_playerlogs_json = spark.table("bronze_riot.playerlogs").filter(
    col("create_room_date") == filter_time
)

orderByDF = bronze_playerlogs_json.orderBy("room_id", "current_time")

windowSpec = Window.partitionBy("encrypted_account").orderBy(col("room_id"))

processingDF = (
    orderByDF.withColumn("prev_x", lag("x", 1).over(windowSpec))
    .withColumn("prev_y", lag("y", 1).over(windowSpec))
    .withColumn(
        "distance",
        round(
            sqrt(pow(col("x") - col("prev_x"), 2) + pow(col("y") - col("prev_y"), 2))
        ),
    )
)

train_data = processingDF.na.fill(0, ["prev_x", "prev_y", "distance"])
train_data = train_data.withColumn(
    "create_room_date", to_date(col("create_room_date"), "yyyy-MM-dd")
)

train_data.write.format("delta").mode("overwrite").partitionBy(
    "create_room_date"
).saveAsTable("silver_train_riot.delta_playerlogs")

spark.stop()
