from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName("PV-merge-test").getOrCreate()

metrics_cols = ["system_id", "metric_id", "sensor_name", "raw_units"]
metrics_data = spark.read.parquet('staging_area/system10/metrics_system10.parquet').select(metrics_cols)
filtered_metrics = metrics_data.filter(functions.col("sensor_name").isin(["dc_power", "ac_power", "poa_irradiance"]))

pv_cols = ["measured_on", "metric_id", "value"]
pv_data = spark.read.parquet('staging_area/system10/pv_data/*').select(pv_cols)

join_condition = pv_data["metric_id"] == filtered_metrics["metric_id"]
merged = pv_data.join(filtered_metrics, join_condition, "inner")

renames = {
        "system_id":"ss_id",
        "measured_on":"timestamp",
        "sensor_name":"sensor",
        "raw_units":"units"
    }
for old_name, new_name in renames.items():
    merged = merged.withColumnRenamed(old_name, new_name)

selected_cols = ["ss_id", "timestamp", "sensor", "units", "value"]
merged = merged.select(selected_cols)
merged = merged.replace("", None)

merged.write.parquet("staging_area/system10/pv_data_merged.parquet")

spark.stop()
