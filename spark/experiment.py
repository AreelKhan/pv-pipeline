from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("experiment").getOrCreate()

# Read data from the mounted directory
data = spark.read.parquet("/opt/bitnami/spark/staging_area/system_10/metrics_system10.parquet")

# Perform your data processing here

# Write the processed data back to the mounted directory
data.write.csv("/opt/bitnami/spark/staging_area/system_10/processed_metrics_system10.parquet")
