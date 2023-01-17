class EcomConfig():
    # S3 bucket for fetching raw data
    S3_BUCKET = "raw-data"
    CURRENT_PATH = None   # will be set to working directory by os.getcwd()

    # Files to download from S3 bucket
    S3_BUCKET_RELEVANT_FILES = {
        "ecom_sales": "ecom_sales_data.csv"
    }
