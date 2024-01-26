from os import path, makedirs, remove
from base_pipeline import BasePipeline, BUCKET_NAME, SITE_PREFIX, MOUNT_PREFIX
import pandas as pd
from logging import Logger


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
            metadata_cols = ["system_id", "latitude", "longitude", "elevation", "av_pressure", "av_temp", "climate_type"]
            metadata = pd.read_parquet(path.join(self.staging_area, "site.parquet"), columns=metadata_cols)

            mount_cols = ["system_id", "azimuth", "tilt"]
            mount_data = pd.read_parquet(path.join(self.staging_area, "mount.parquet"), columns=mount_cols)

            merged = pd.merge(metadata, mount_data, "inner", "system_id").rename({"system_id":"ss_id", "azimuth":"mount_azimuth", "tilt":"mount_tilt"}, axis=1)
            merged.to_parquet(path.join(self.staging_area, "metadata.parquet"))
            
            remove(path.join(self.staging_area, "site.parquet"))
            remove(path.join(self.staging_area, "mount.parquet"))
        except Exception as error:
            self.logger.error(f"Error while transforming metadata: \n{error}")

        return None