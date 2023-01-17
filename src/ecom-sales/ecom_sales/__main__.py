import sys
import logging
import argparse

from ecom_sales.api import extract

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def main(args):
    try:
        if sys.argv[1] == "extract":
            extract()
    except IndexError:
        raise IndexError("Call to API requires an endpoint")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data Pipeline"
    )
    args = parser.parse_args(sys.argv[2:])
    main(args)