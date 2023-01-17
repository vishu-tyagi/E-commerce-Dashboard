import os
from pathlib import Path
import logging

import boto3
import pandas as pd
from sqlalchemy.engine.base import Engine

from ecom_sales.config import EcomConfig
from ecom_sales.data_access.helpers import s3_download, upload_to_postgres
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

    def extract(self, client: boto3.client, dataset: str):
        logger.info(f"Fetching dataset {dataset} ...")
        for fname in self.config.S3_BUCKET_RELEVANT_FILES[dataset]:
            s3_download(
                client=client,
                bucket=self.s3_bucket,
                filename=self.config.S3_BUCKET_RELEVANT_FILES[dataset][fname],
                outpath=os.path.join(self.data_path, fname)
            )

    def load(
        self,
        dataset: str,
        engine: Engine,
        schema: str
    ):
        logger.info(
            f"Ingesting {dataset} into table {schema}.{dataset} on {engine.url} ..."
        )
        if_table_exists = "replace"
        nrows = 0
        for fname in self.config.S3_BUCKET_RELEVANT_FILES[dataset]:
            df = pd.read_csv(os.path.join(self.data_path, fname))
            df.columns = [c.lower() for c in df.columns]
            upload_to_postgres(
                df=df,
                engine=engine,
                schema=schema,
                table=dataset,
                if_table_exists=if_table_exists,
                chunksize=10000,
                filename=fname
            )
            if_table_exists = "append"
            nrows += df.shape[0]
        logger.info(f"Completed ingesting {nrows} rows")