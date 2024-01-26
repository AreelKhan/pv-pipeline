import glob
import logging
from datetime import datetime
from logging import Logger
from os import makedirs, path, remove
from shutil import rmtree
from typing import List

import boto3
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from pyspark.sql import SparkSession, functions

# S3 bucket and paths
BUCKET_NAME = "oedi-data-lake"

PARENT_PREFIX = "pvdaq/parquet/"
SITE_PREFIX = PARENT_PREFIX + "site/"
MOUNT_PREFIX =  PARENT_PREFIX + "mount/"

METRICS_PREFIX = PARENT_PREFIX + "metrics/metrics__system_{ss_id}"
PV_PREFIX = PARENT_PREFIX + "pvdata/system_id={ss_id}/year={year}/month={month}/day={day}/"


class BaseExtractor:
    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            region_name: str,
            staging_area: str,
            logger: Logger
            ):
        """
            Initializes the Extract step of the data pipeline
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        self.staging_area = staging_area
        self.logger = logger


    def s3_download(self, key: str, filename: str):
        """
            Given an AWS S3 file key, downloads it.
            File is named filename.
            Assumes filename has valid file path. (director already exists)
        """
        self.s3.download_file(BUCKET_NAME, key, filename)


class BaseLoader:
     
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
        job_config.write_disposition = "WRITE_APPEND"
        job_config.schema_update_options = ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION']

        with open(source_data_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()

        return None


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

























extractor = PVExtract(
    staging_area="staging_area",
    aws_access_key_id="AKIA4MTWG33OOIEEML5D",
    aws_secret_access_key="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",
    region_name="us-west-2",
    logger=logging.getLogger(__name__)
)

ss_id = 10
start_date = datetime.strptime("2010/03/01", "%Y/%m/%d")
end_date = datetime.strptime("2010/03/10", "%Y/%m/%d")

extractor.extract(
    ss_id=ss_id,
    start_date=start_date,
    end_date=end_date
)


transformer = PVTransform(
    staging_area="./staging_area/",
    logger=logging.getLogger(__name__)
)
transformer.transform(ss_id)


loader = PVLoad(
    project_id="cohere-pv-pipeline",
    credentials_path="./bq_service_account_key.json",
    staging_area="staging_area",
    logger=logging.getLogger(__name__)
    )
loader.load(ss_id)