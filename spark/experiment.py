from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PV-merge-test").getOrCreate()

# Define the column names
pv_cols = ["measured_on", "metric_id", "value"]

# Read parquet files into a DataFrame
pv_data = spark.read.parquet('staging_area/system_1349/pv_data/pv_data_system1349_2013-05-01.parquet').select(pv_cols)

# Show the DataFrame
pv_data.show()

# Stop the Spark session
spark.stop()
