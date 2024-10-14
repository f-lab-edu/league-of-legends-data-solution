from pyspark.sql import SparkSession

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder.appName("ML-Pipeline-Kmeans")
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
    .config("spark.executor.memory", "2G")
    .config("spark.executor.instances", "2")
    .enableHiveSupport()
    .getOrCreate()
)

# Silver Layer Train Data 가져오기
train_data = spark.table("silver_train_riot.delta_playerlogs")

# Feature Vector 만들기
assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features_vector")

# KMeans 설정
kmeans = KMeans(k=2, seed=1, featuresCol="features_vector", predictionCol="prediction")

pipeline = Pipeline(stages=[assembler, kmeans])

pipelineModel = pipeline.fit(train_data)
train_data_prediction = pipelineModel.transform(train_data)

evaluator = ClusteringEvaluator(
    featuresCol="features_vector", predictionCol="prediction", metricName="silhouette"
)

silhouette = evaluator.evaluate(train_data_prediction)

print(f"Score: {silhouette}")

pipelineModel.write().overwrite().save("s3://sjm-simple-data/app/model_001/")
