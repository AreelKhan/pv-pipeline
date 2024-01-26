from os import path, makedirs, remove
from logging import Logger
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from typing import List

from base_pipeline import BasePipeline, BUCKET_NAME, SITE_PREFIX, MOUNT_PREFIX

class MetadataExtract(BasePipeline):

    def extract(self) -> None:
        """
            Extracts PV system metadata
        """
        self.logger.info("Extracting metadata from source...")
        makedirs(self.staging_area, exist_ok=True)

        site_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=SITE_PREFIX, Delimiter="/")
        try:
            site_key = site_object["Contents"][0]["Key"]
            self.s3_download(site_key, path.join(self.staging_area, f"site.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting site data: \n{error}")

        mount_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=MOUNT_PREFIX, Delimiter="/")
        try:
            mount_key = mount_object["Contents"][0]["Key"]
            self.s3_download(mount_key, path.join(self.staging_area, f"mount.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting mount data: \n{error}")

        return None


class MetadataTransform():

    def __init__(self, staging_area: str, logger: Logger):
        self.staging_area = staging_area
        self.logger = logger

    def transform(self):
        try:
            self.logger.info("Transforming metadata...")
            
            site_cols = ["system_id", "latitude", "longitude", "elevation", "av_pressure", "av_temp", "climate_type"]
            site_data = pd.read_parquet(path.join(self.staging_area, "site.parquet"), columns=site_cols)

            mount_cols = ["system_id", "azimuth", "tilt"]
            mount_data = pd.read_parquet(path.join(self.staging_area, "mount.parquet"), columns=mount_cols)

            merged = pd.merge(site_data, mount_data, "inner", "system_id")
            merged = merged.rename({"system_id":"ss_id", "azimuth":"mount_azimuth", "tilt":"mount_tilt"}, axis=1)
            merged = merged.replace("", pd.NA)
            for col in merged.columns:
                if col != "climate_type":
                    merged[col] = pd.to_numeric(merged[col], "coerce")

            merged.to_parquet(path.join(self.staging_area, "metadata.parquet"))
            remove(path.join(self.staging_area, "site.parquet"))
            remove(path.join(self.staging_area, "mount.parquet"))

        except Exception as error:
            self.logger.error(f"Error while transforming metadata: \n{error}")

        return None
    

class MetadataLoad:
     
    def __init__(
            self,
            project_id: str,
            credentials_path: str,
            staging_area: str,
            logger: Logger,
            ):
        self.project_id = project_id
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(project=project_id, credentials=self.credentials)
        self.staging_area = staging_area
        self.logger = logger

    def load_to_bq(
            self,
            dataset_id: str,
            table_id: str,
            table_schema: List[bigquery.SchemaField],
            source_data_path: str,
            ):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=table_schema)

        try:
            self.client.get_table(table)
        except Exception:
            self.logger.info(f"Table {table} is not found. Creating...")
            self.client.create_table(table)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET

        with open(source_data_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()

        return None

    def load(self):
        table_schema = [
            bigquery.SchemaField(name="ss_id", field_type="INTEGER", mode="NULLABLE"),
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

        return None
