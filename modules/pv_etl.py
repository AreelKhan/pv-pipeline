import glob
from datetime import datetime
from logging import Logger
from os import makedirs, path, remove
from shutil import rmtree

import pandas as pd
from base_classes import (BUCKET_NAME, METRICS_PREFIX, PV_PREFIX,
                           BaseExtractor, BaseLoader)
from google.cloud import bigquery
from pyspark.sql import SparkSession, functions


class PVExtract(BaseExtractor):

    def extract_metrics(self, ss_id: int) -> None:
        """
            Extracts Metrics given an ss_id
        """
        metrics_aws_path = METRICS_PREFIX.replace("{ss_id}", str(ss_id))
        metrics_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=metrics_aws_path, Delimiter="/")
        try:
            metrics_key = metrics_object["Contents"][0]["Key"]
            local_dir = path.join(self.staging_area, f"system{ss_id}")
            makedirs(local_dir, exist_ok=True)
            self.s3_download(metrics_key, path.join(local_dir, f"metrics_system{ss_id}.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting metrics for Site {ss_id}: \n{error}")


    def extract_pv_data(self, ss_id: int, date: datetime) -> None:
        """
            Extracts PV data given an ss_id and date (single day)
        """
        pv_aws_path = PV_PREFIX.replace("{ss_id}", str(ss_id)).replace("{year}", str(date.year)).replace("{month}", str(date.month)).replace("{day}", str(date.day))
        pv_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=pv_aws_path, Delimiter="/")
        try:
            pv_data_key = pv_object["Contents"][0]["Key"]
            local_dir = path.join(self.staging_area, f"system{ss_id}", "pv_data")
            makedirs(local_dir, exist_ok=True)
            self.s3_download(pv_data_key, path.join(local_dir, f"pv_data_system{ss_id}_{date.strftime('%Y-%m-%d')}.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting PV data for Site {ss_id} on {date}: /n{error}")


    def extract(self, ss_id: int, start_date: datetime, end_date: datetime) -> None:
        """
            Extracts PV data and metrics for a given ss_id and date
        """
        makedirs(self.staging_area, exist_ok=True)

        self.logger.info(f"Extracting Metrics for System {ss_id}...")
        self.extract_metrics(ss_id)
        
        self.logger.info(f"Extracting PV data for System {ss_id} for dates: {start_date} to {end_date}")
        for date in pd.date_range(start=start_date, end=end_date):
            self.extract_pv_data(ss_id, date)


class PVTransform:

    def __init__(self, staging_area: str, logger: Logger):
        self.staging_area = staging_area
        self.logger = logger

    def transform(self, ss_id: int):
        """
            Transforms all the data present in the staging area for ss_id
        """
        try:
            self.logger.info(f"Transforming PV data for System {ss_id}...") 
            spark = SparkSession.builder.appName(f"PV_system{ss_id}_transform").getOrCreate()

            metrics_cols = ["system_id", "metric_id", "sensor_name", "raw_units"]
            metrics_data = spark.read.parquet(path.join(self.staging_area, f"system{ss_id}", f"metrics_system{ss_id}.parquet")).select(metrics_cols)
            filtered_metrics = metrics_data.filter(functions.col("sensor_name").isin(["dc_power", "ac_power", "poa_irradiance"]))

            pv_cols = ["measured_on", "metric_id", "value"]
            pv_data = spark.read.parquet(path.join(self.staging_area, f"system{ss_id}", "pv_data", "*")).select(pv_cols)

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

            merged.write.parquet(path.join(self.staging_area, f"system{ss_id}", "pv_data_merged.parquet"))

            spark.stop()

            rmtree(path.join(self.staging_area, f"system{ss_id}", "pv_data/"))
            remove(path.join(self.staging_area, f"system{ss_id}", f"metrics_system{ss_id}.parquet"))
                   
        except Exception as error:
            self.logger.error(f"Error while transforming PV data for system {ss_id}: \n{error}")


        return None
    

class PVLoad(BaseLoader):

    def load(self, ss_id: int):
        table_schema = [
            bigquery.SchemaField("ss_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("timetsamp", "DATETIME"),
            bigquery.SchemaField("sensor", "STRING"),
            bigquery.SchemaField("units", "STRING"),
            bigquery.SchemaField("value", "FLOAT"),
            ]
        parquet_files = glob.glob(path.join(self.staging_area, f"system{ss_id}", "pv_data_merged.parquet", "*.parquet"))
        self.logger.info(f"Uploading PV data for system {ss_id}")
        for file in parquet_files:
            try:
                self.load_to_bq(
                    dataset_id="pv_oedi",
                    table_id="pv_data",
                    table_schema=table_schema,
                    source_data_path=file
                )
                
            except Exception as error:
                self.logger.error(f"Error while loading PV data from {file} into BigQuery: \n{error}")
                raise error

        return None
