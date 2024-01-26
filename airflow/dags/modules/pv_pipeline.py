from os import path, makedirs, remove
from shutil import rmtree
from logging import Logger
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
from base_pipeline import BaseExtractor, BaseLoader, BUCKET_NAME, METRICS_PREFIX, PV_PREFIX

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
            self.logger.error(f"Error while extracting PV data for Site {ss_id} on {date}: \n{error}")


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
            
            metrics_cols = ["system_id", "metric_id", "sensor_name", "raw_units"]
            metrics = pd.read_parquet(path.join(self.staging_area, f"system{ss_id}", f"metrics_system{ss_id}.parquet"), columns=metrics_cols)
            metrics = metrics[metrics["sensor_name"].isin(["dc_power", "ac_power", "poa_irradiance"])]

            pv_cols = ["measured_on", "metric_id", "value"]
            pv_data = pd.read_parquet(path.join(self.staging_area, f"system{ss_id}", "pv_data", f"pv_data_system{ss_id}_2010-03-09.parquet"), columns=pv_cols)

            renames = {
                    "system_id":"ss_id",
                    "measured_on":"timestamp",
                    "sensor_name":"sensor",
                    "raw_units":"units"
                }
            merged = pd.merge(pv_data, metrics, "inner", "metric_id")
            merged = merged.rename(renames, axis=1)
            merged = merged[["ss_id", "units", "timestamp", "value", "sensor"]]
            merged = merged.replace("", pd.NA)

            for col in merged.columns:
                if col not in ["sensor", "units", "timestamp"]:
                    merged[col] = pd.to_numeric(merged[col], "coerce")

            merged.to_parquet(path.join(self.staging_area, "system10", "pv_data_merged.parquet"))
            rmtree(path.join(self.staging_area, f"system{ss_id}", "pv_data/"))
            remove(path.join(self.staging_area, f"system{ss_id}", f"metrics_system{ss_id}.parquet"))

        except Exception as error:
            self.logger.error(f"Error while transforming metadata: \n{error}")

        return None
    

class MetadataLoad(BaseLoader):

    def load(self):
        table_schema = [
            bigquery.SchemaField("ss_id", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("elevation", "FLOAT"),
            bigquery.SchemaField("av_pressure", "FLOAT"),
            bigquery.SchemaField("av_temp", "FLOAT"),
            bigquery.SchemaField("climate_type", "STRING"),
            bigquery.SchemaField("mount_azimuth", "FLOAT"),
            bigquery.SchemaField("mount_tilt", "FLOAT")
            ]
        try:
            self.load_to_bq(
                dataset_id="pv_oedi",
                table_id="pv_metadata",
                table_schema=table_schema,
                source_data_path=path.join(self.staging_area, "metadata.parquet")
            )
        except Exception as error:
            self.logger.error(f"Error while loading metadata into BigQuery: \n{error}")
            raise error

        return None
