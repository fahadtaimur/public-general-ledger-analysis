"""
List objects in an S3 bucket
"""

import logging
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
)

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def list_objects_in_bucket(
    bucket_name: str,
    s3_client: Optional["S3Client"] = None,
) -> List[str]:
    """
    List all objects in an S3 bucket

    :param bucket_name: Name of the bucket
    :return: List of S3 bucket object keys
    """

    s3_client = s3_client or boto3.client("s3")

    # Reference: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/list_objects_v2.html
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    file_object_keys: List[str] = []
    try:
        for page in pages:
            if "Contents" in page:
                file_object_keys.extend(p["Key"] for p in page["Contents"])
        return file_object_keys
    except ClientError as e:
        logging.error("Failed to retrieve object keys from bucket %s: %s", bucket_name, str(e))
        raise
