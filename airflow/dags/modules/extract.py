import boto3
from os import path, makedirs
import json
from datetime import datetime
from logging import Logger
import pandas as pd
from tqdm import tqdm

# S3 bucket and paths
BUCKET_NAME = "oedi-data-lake"

PARENT_PREFIX = "pvdaq/parquet/"
SITE_PREFIX = PARENT_PREFIX + "site/"
MOUNT_PREFIX =  PARENT_PREFIX + "mount/"

METRICS_PREFIX = PARENT_PREFIX + "metrics/metrics__system_{ss_id}"
PV_PREFIX = PARENT_PREFIX + "pvdata/system_id={ss_id}/year={year}/month={month}/day={day}/"

class PVExtract:

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


    def extract_metadata(self) -> None:
        """
            Extracts PV system metadata
        """
        site_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=SITE_PREFIX, Delimiter="/")
        makedirs(self.staging_area, exist_ok=True)

        try:
            site_key = site_object["Contents"][0]["Key"]
            self.s3_download(site_key, path.join(self.staging_area, f"metadata.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting metadata: \n{error}")

        mount_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=MOUNT_PREFIX, Delimiter="/")
        try:
            mount_key = mount_object["Contents"][0]["Key"]
            self.s3_download(mount_key, path.join(self.staging_area, f"mount.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting mount data: \n{error}")


    def extract_metrics(self, ss_id: int) -> None:
        """
            Extracts Metrics given an ss_id
        """
        metrics_aws_path = METRICS_PREFIX.replace("{ss_id}", str(ss_id))
        metrics_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=metrics_aws_path, Delimiter="/")
        try:
            metrics_key = metrics_object["Contents"][0]["Key"]
            local_dir = path.join(self.staging_area, f"system_{ss_id}")
            makedirs(local_dir, exist_ok=True)
            self.s3_download(metrics_key, path.join(local_dir, f"metrics_system{ss_id}.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting metrics for Site {ss_id}: \n{error}")


    def extract_pv_data(self, ss_id: int, date: datetime) -> None:
        """
            Extracts PV data given an ss_id and date
        """
        pv_aws_path = PV_PREFIX.replace("{ss_id}", str(ss_id)).replace("{year}", str(date.year)).replace("{month}", str(date.month)).replace("{day}", str(date.day))
        pv_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=pv_aws_path, Delimiter="/")
        try:
            pv_data_key = pv_object["Contents"][0]["Key"]
            local_dir = path.join(self.staging_area, f"system_{ss_id}", "pv_data")
            makedirs(local_dir, exist_ok=True)
            self.s3_download(pv_data_key, path.join(local_dir, f"pv_data_system{ss_id}_{date.strftime('%Y-%m-%d')}.parquet"))
        except Exception as error:
            self.logger.error(f"Error while extracting PV data for Site {ss_id} on {date}: \n {error}")


    def extract(self, ss_id: int, start_date: datetime, end_date: datetime) -> None:
        """
            Extracts PV data and associated metrics, metadata and mount data for a given ss_id and date
        """
        # create staging area if it does not exist
        makedirs(self.staging_area, exist_ok=True)

        # check if system metadata exists, if not extract
        if not path.isfile(path.join(self.staging_area, "metadata.parquet")):
            self.logger.info("Metadata is not available. Extracting from source...")
            self.extract_metadata()

        # check if metadata exists, if not extract
        if not path.isfile(path.join(self.staging_area, f"system_{ss_id}", f"metrics_system{ss_id}.parquet")):
            self.logger.info(f"Metrics for System {ss_id} are not available. Extracting from source...")
            self.extract_metrics(ss_id)
        
        # extract pv
        self.logger.info(f"Extracting PV data for System {ss_id} for dates: {start_date} to {end_date}")
        for date in tqdm(pd.date_range(start=start_date, end=end_date)):
            self.extract_pv_data(ss_id, date)