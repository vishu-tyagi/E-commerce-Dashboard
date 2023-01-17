import os
import logging

import boto3
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema

from ecom_sales.config import EcomConfig
from ecom_sales.data_access import DataPipeline
from ecom_sales.utils import timing

logger = logging.getLogger(__name__)


@timing
def extract(config: EcomConfig = EcomConfig) -> None:
    s3_client = \
        boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )
    data = DataPipeline(config)
    data.make_dirs()
    for dataset in config.S3_BUCKET_RELEVANT_FILES:
        data.extract(client=s3_client, dataset=dataset)
    return


@timing
def load(
    connection_string: str,
    schema: str,
    config: EcomConfig = EcomConfig
) -> None:
    engine = create_engine(connection_string)
    if not engine.dialect.has_schema(engine, schema):
        engine.execute(CreateSchema(schema))
        logger.info(f"Created new schema {schema}")
    data = DataPipeline(config)
    for dataset in config.S3_BUCKET_RELEVANT_FILES:
        data.load(
            dataset=dataset,
            engine=engine,
            schema=schema
        )
    return