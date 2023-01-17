import os
import logging
import logging.config
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy.engine.base import Engine

from ecom_sales.utils import timing

logger = logging.getLogger(__name__)


@timing
def s3_download(
    client: boto3.client,
    bucket: str,
    filename: str,
    outpath: Path
):
    client.download_file(
        bucket,
        filename,
        outpath
    )
    logger.info(f"Downloaded s3://{bucket}/{filename}")


@timing
def upload_to_postgres(
    df: pd.DataFrame,
    engine: Engine,
    schema: str,
    table: str,
    if_table_exists: str,
    chunksize: int,
    filename: str
):
    logger.info(f"Ingesting {filename} with {df.shape[0]} rows ...")
    df.to_sql(
        name=table,
        con=engine,
        if_exists=if_table_exists,
        index=False,
        chunksize=chunksize,
        schema=schema
    )
    return