"""
Get General Ledger CSV from Oklahoma Government website.
"""

import logging
from typing import (
    List,
    Tuple,
)

import requests
from requests.models import Response

from src.config.constants import (
    API_URL,
    GENERAL_LEDGER_ID,
)


def fetch_oklahoma_gl_resources(timeout: int = 60) -> List[dict]:
    """
    Fetches full resource metadata for all General Ledger CSV files from the CKAN API.

    This function sends a GET request to the Oklahoma Government API and returns
    the complete resource metadata for each CSV file. The full metadata is used
    downstream for DynamoDB storage and change detection.

    :param timeout: Timeout settings for the HTTP request, in seconds.
    :type timeout: int
    :return: A list of resource metadata dicts from the CKAN API.
    :rtype: List[dict]
    """
    try:
        response: Response = requests.get(url=API_URL, params={"id": GENERAL_LEDGER_ID}, timeout=timeout)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict) or "result" not in data or "resources" not in data["result"]:
            logging.error("Unexpected API response structure: %s", data)
            raise ValueError("API response does not contain 'result' or 'resources'.")

        resources = [r for r in data["result"]["resources"] if r["url"].lower().endswith(".csv")]
        logging.info("Successfully fetched %d resource(s) from API.", len(resources))
        return resources
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch resources from API. Error: %s", e)
        raise


def fetch_oklahoma_gl_csv_from_url(resource: dict, timeout: int = 60) -> Tuple[str, bytes]:
    """
    Fetches a General Ledger CSV file from the given resource metadata.

    This function retrieves a CSV file from the Oklahoma Government website
    based on the resource metadata dict. The file content is returned
    as binary data along with the file name.

    :param resource: A resource metadata dict containing at minimum 'name' and 'url'.
    :type resource: dict
    :param timeout: Timeout settings for the HTTP request.
    :type timeout: int
    :return: A tuple containing the name of the fetched file and data as binary content
    :rtype: Tuple[str, bytes]
    """
    file_name = resource["name"]
    file_url = resource["url"]

    try:
        response: Response = requests.get(url=file_url, stream=True, timeout=timeout)
        response.raise_for_status()
        logging.info("Successfully fetched file '%s'", file_name)
        return file_name, response.content
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch file '%s' from URL: %s. Error: %s", file_name, file_url, e)
        raise
