import logging

from ecom_sales.config import EcomConfig
from ecom_sales.data_access import DataPipeline
from ecom_sales.utils import timing

logger = logging.getLogger(__name__)


@timing
def extract(config: EcomConfig = EcomConfig) -> None:
    data = DataPipeline(config)
    data.make_dirs()
    data.extract()
    return