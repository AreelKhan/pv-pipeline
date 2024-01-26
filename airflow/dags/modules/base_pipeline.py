import boto3
from os import path, makedirs
from datetime import datetime
from logging import Logger
import pandas as pd

# S3 bucket and paths
BUCKET_NAME = "oedi-data-lake"

PARENT_PREFIX = "pvdaq/parquet/"
SITE_PREFIX = PARENT_PREFIX + "site/"
MOUNT_PREFIX =  PARENT_PREFIX + "mount/"

METRICS_PREFIX = PARENT_PREFIX + "metrics/metrics__system_{ss_id}"
PV_PREFIX = PARENT_PREFIX + "pvdata/system_id={ss_id}/year={year}/month={month}/day={day}/"


class BasePipeline:
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