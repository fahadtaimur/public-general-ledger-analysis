"""
Upload objects to S3
"""

import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import ResponseMetadataTypeDef


def write_csv_object_to_s3(
    bucket_name: str,
    object_key: str,
    file_data: bytes,
    content_type: str = "text/csv",
    s3_client: Optional["S3Client"] = None,
) -> ResponseMetadataTypeDef:
    """
    Upload a csv file to AWS S3.

    :param bucket_name: The name of the S3 bucket.
    :type bucket_name: str
    :param object_key: The object key (path) in bucket.
    :type object_key: str
    :param file_data: The csv file content to upload.
    :type file_data: bytes
    :param content_type: The content type of the file. Defaults to "text/csv"
    :type content_type: str
    :param s3_client: A custom S3 client. If not provided, a default client is created.
    """

    s3_client = s3_client or boto3.client("s3")

    try:
        response = s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=file_data, ContentType=content_type)
        logging.info("Successfully uploaded %s to bucket %s", object_key, bucket_name)
        return response["ResponseMetadata"]
    except ClientError as e:
        logging.error("Failed to upload %s to bucket %s: %s", object_key, bucket_name, str(e))
        raise
