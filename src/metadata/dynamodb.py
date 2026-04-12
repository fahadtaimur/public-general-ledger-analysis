"""
Module for interaction with dynamodb which is used for metadata storage
"""

import logging
from typing import (
    TYPE_CHECKING,
    Optional,
)

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from config.constants import (
    DYNAMODB_TABLENAME,
    REGION,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import (
        GetItemOutputTypeDef,
        PutItemOutputTypeDef,
    )


def get_metadata_item(
    resource_id: str, table_name: str = DYNAMODB_TABLENAME, client: Optional["DynamoDBClient"] = None
) -> "GetItemOutputTypeDef":
    """
    Retrieve a metadata record from DynamoDB by resource_id.

    :param table_name: The name of the DynamoDB table.
    :type table_name: str
    :param resource_id: The resource_id to look up (partition key).
    :type resource_id: str
    :param client: A custom DynamoDB client. If not provided, a default client is created.
    :type client: Optional[DynamoDBClient]
    :return: The DynamoDB response. If the item exists, it is in response["Item"].
    :rtype: GetItemOutputTypeDef
    """
    client = client or boto3.client(service_name="dynamodb", region_name=REGION)

    try:
        response = client.get_item(TableName=table_name, Key={"resource_id": {"S": resource_id}})
        return response
    except ClientError as e:
        logging.error("Failed to get item with resource_id %s from table %s: %s", resource_id, table_name, str(e))
        raise


def _serialize_object_for_dynamodb(resource: dict, retrieved_at: str) -> dict:
    """
    Serialize a resource metadata dict into DynamoDB's typed format.

    Merges retrieved_at into the resource dict and converts each value to its
    DynamoDB type representation (e.g. str -> {"S": ...}, int -> {"N": ...},
    bool -> {"BOOL": ...}, None -> {"NULL": True}).

    :param resource: The full resource metadata dict from the CKAN API.
    :type resource: dict
    :param retrieved_at: ISO timestamp of when the file was retrieved.
    :type retrieved_at: str
    :return: A dict with DynamoDB-typed values ready for put_item.
    :rtype: dict
    """
    serializer = TypeSerializer()
    item = {**resource, "retrieved_at": retrieved_at}
    return {k: serializer.serialize(v) for k, v in item.items()}


def put_metadata_item(
    resource: dict,
    retrieved_at: str,
    table_name: str = DYNAMODB_TABLENAME,
    client: Optional["DynamoDBClient"] = None,
) -> "PutItemOutputTypeDef":
    """
    Write a metadata record to DynamoDB.

    :param resource: The full resource metadata dict from the CKAN API.
    :type resource: dict
    :param retrieved_at: ISO timestamp of when the file was retrieved.
    :type retrieved_at: str
    :param table_name: The name of the DynamoDB table.
    :type table_name: str
    :param client: A custom DynamoDB client. If not provided, a default client is created.
    :type client: Optional[DynamoDBClient]
    :return: The DynamoDB response metadata.
    :rtype: PutItemOutputTypeDef
    """
    client = client or boto3.client(service_name="dynamodb", region_name=REGION)

    item = _serialize_object_for_dynamodb(resource, retrieved_at)

    try:
        response = client.put_item(TableName=table_name, Item=item)
        return response
    except ClientError as e:
        logging.error(
            "Failed to write metadata for resource_id %s to table %s: %s", resource["resource_id"], table_name, str(e)
        )
        raise
