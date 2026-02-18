"""
This script runs the data pipeline for fetching and uploading Oklahoma GL data.
"""

import logging

from src.config.paths import (
    S3_BUCKET_NAME,
    S3_RAW_DIR,
)
from src.data.fetch_data import (
    fetch_oklahoma_gl_csv_from_url,
    fetch_oklahoma_gl_urls,
)
from src.s3.write_object import write_csv_object_to_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")


def run():
    # Retrieve file name and urls
    file_info_metadata = fetch_oklahoma_gl_urls()
    print(file_info_metadata)

    # Test with a single file
    file_info = (
        "LEDGER_FY25_QTR2.csv",
        "https://data.ok.gov/dataset/dd1ecf41-4abc-4886-ab0f-b84d7662d8d4/resource/b1b031e1-d26d-4f1f-8892-4fcec01031dc/download/ledger_fy25_qtr2.csv",
    )

    file_name, file_data = fetch_oklahoma_gl_csv_from_url(file_info=file_info)

    # Upload to S3
    write_csv_object_to_s3(bucket_name=S3_BUCKET_NAME, object_key=f"{S3_RAW_DIR}/{file_name}", file_data=file_data)


if __name__ == "__main__":
    run()
