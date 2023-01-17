class EcomConfig():
    # S3 bucket for fetching raw data
    S3_BUCKET = "raw-data-vt"
    CURRENT_PATH = None   # will be set to working directory by os.getcwd()

    # Files to download from S3 bucket
    S3_BUCKET_RELEVANT_FILES = {
        "ecom_sales": {
            "ecom_sales.csv": "ecom_sales.csv"
        }
    }
