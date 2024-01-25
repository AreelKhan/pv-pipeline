import boto3
from os import path, makedirs
import json
from datetime import datetime, timedelta
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
aws_secrets = json.load(open("secrets.json"))


# S3 bucket and paths
BUCKET_NAME = "oedi-data-lake"

PARENT_PREFIX = "pvdaq/parquet/"
SITE_PREFIX = PARENT_PREFIX + "site/"
MOUNT_PREFIX =  PARENT_PREFIX + "mount/"

METRICS_PREFIX = PARENT_PREFIX + "metrics/metrics__system_{ss_id}"
PV_PREFIX = PARENT_PREFIX + "pvdata/system_id={ss_id}/year={year}/month={month}/day={day}/"

# Local location for temporary data storage
STAGING_AREA = './data'

class PVExtract:

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )


    def s3_download(self, key: str, filename: str):
        """
            Given an AWS S3 file key, download and store in the staging destination.
            File is named filename
        """
        self.s3.download_file(BUCKET_NAME, key, filename)

    def extract_metadata(self) -> None:
        """
            Extracts PV system metadata
        """
        pass

    def extract_metrics(self, ss_id: int) -> None:
        """
            Extracts metrics given an ss_id
        """
        metrics_aws_path = METRICS_PREFIX.replace("{ss_id}", str(ss_id))
        metrics_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=metrics_aws_path, Delimiter="/")
        try:
            metrics_key = metrics_object["Contents"][0]["Key"]
            local_dir = path.join(STAGING_AREA, f"system_{ss_id}")
            makedirs(local_dir, exist_ok=True)
            self.s3_download(metrics_key, path.join(local_dir, f"metrics_system{ss_id}.parquet"))
        except:
            logging.error(f"Metrics for system: {ss_id} were not found. Aborting extraction.")
        
    def extract_pv_data(self, ss_id: int, date: datetime) -> None:
        """
            Extracts pv data given an ss_id and date
        """
        pv_aws_path = PV_PREFIX.replace("{ss_id}", str(ss_id)).replace("{year}", str(date.year)).replace("{month}", str(date.month)).replace("{day}", str(date.day))
        pv_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=pv_aws_path, Delimiter="/")
        try:
            pv_data_key = pv_object["Contents"][0]["Key"]
            local_dir = path.join(STAGING_AREA, f"system_{ss_id}", "pv_data")
            makedirs(local_dir, exist_ok=True)
            self.s3_download(pv_data_key, path.join(local_dir, f"pv_data_system{ss_id}_{date.strftime("%Y-%m-%d")}.parquet"))
        except:
            logging.error(f"PV data for System: {ss_id} on {date} was not found. Aborting extraction.")

    def extract(self, ss_id: int, start_date: datetime, end_date: datetime) -> None:
        """
            Extracts pv data and associated metrics for given ss_id and date
        """
        # create staging area if it does not exist
        makedirs(STAGING_AREA, exist_ok=True)

        # check if system metadata exists, if not extract
        if not path.isfile(path.join(STAGING_AREA, "metadata.parquet")):
            logging.info("Metadata is not available. Extracting from source...")
            self.extract_metadata()

        # check if metadata exists, if not extract
        if not path.isfile(path.join(STAGING_AREA, f"system_{ss_id}", f"metrics_system{ss_id}.parquet")):
            logging.info(f"Metrics for System {ss_id} are not available. Extracting from source...")
            self.extract_metrics(ss_id)
        
        # extract pv
        logging.info(f"Extracting PV data for System {ss_id} for dates: {start_date} to {end_date}")
        for date in pd.date_range(start=start_date, end=end_date):
            self.extract_pv_data(ss_id, date)
