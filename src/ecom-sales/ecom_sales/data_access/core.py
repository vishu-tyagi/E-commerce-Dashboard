import os
from pathlib import Path
import logging

import boto3
import pandas as pd

from ecom_sales.config import EcomConfig
from ecom_sales.data_access.helpers import s3_download
from ecom_sales.utils import timing
from ecom_sales.utils.constants import (
    DATA_DIR
)

logger = logging.getLogger(__name__)


class DataPipeline():
    def __init__(self, config: EcomConfig):
        self.config = config
        self.s3_bucket = config.S3_BUCKET
        self.current_path = Path(os.getcwd()) if not config.CURRENT_PATH else config.CURRENT_PATH
        self.data_path = Path(os.path.join(self.current_path, DATA_DIR))

    def make_dirs(self):
        dirs = [self.data_path]
        for dir in dirs:
            dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created data directory {self.data_path}")

    @timing
    def extract(self):
        logger.info(f"Fetching raw data ...")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )
        s3_download(
            s3_client,
            self.s3_bucket,
            self.config.S3_BUCKET_RELEVANT_FILES,
            self.data_path
        )
        logger.info(f"Data available at {self.data_path}")