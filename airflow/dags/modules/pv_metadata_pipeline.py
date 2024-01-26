from os import path, makedirs
from base_pipeline import BasePipeline, BUCKET_NAME, SITE_PREFIX, MOUNT_PREFIX


class MetadataExtract(BasePipeline):

    def extract_metadata(self) -> None:
        """
            Extracts PV system metadata
        """
        makedirs(self.staging_area, exist_ok=True)
        site_object = self.s3.list_objects(Bucket=BUCKET_NAME, Prefix=SITE_PREFIX, Delimiter="/")

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


    def extract(self) -> None:
        """
            Extracts PV data and associated metrics, metadata and mount data for a given ss_id and date
        """
        # create staging area if it does not exist
        makedirs(self.staging_area, exist_ok=True)

        # check if system metadata exists, if not extract
        if not path.isfile(path.join(self.staging_area, "metadata.parquet")):
            self.logger.info("Metadata is not available. Extracting from source...")
            self.extract_metadata()
