import boto3
from os import path
from logging import Logger
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import List

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
