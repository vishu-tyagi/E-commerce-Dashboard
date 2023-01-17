import os
import logging
import logging.config
from pathlib import Path
from typing import List

from ecom_sales.utils import timing

logger = logging.getLogger(__name__)


@timing
def s3_download(
    client,
    bucket: str,
    file_dict: dict,
    out_path: Path
):
    for fname in file_dict:
        client.download_file(
            bucket,
            file_dict[fname],
            os.path.join(out_path, fname)
        )
        logger.info(f"Downloaded s3://{bucket}/{fname}")