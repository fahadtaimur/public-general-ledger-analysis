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


def fetch_oklahoma_gl_urls(timeout: int = 60) -> List[Tuple[str, str]]:
    """
    Fetches a list of General Ledger CSV file names and their download URLs.

    This function sends a GET request to the Oklahoma Government API to retrieve
    metadata about General Ledger files. It extracts the file names and their
    corresponding download URLs from the API response.

    :param timeout: Timeout settings for the HTTP request, in seconds.
    :type timeout: int
    :return: A list of tuples, where each tuple contains a file name and its download URL.
    :rtype: List[Tuple[str, str]]
    """
    try:
        response: Response = requests.get(url=API_URL, params={"id": GENERAL_LEDGER_ID}, timeout=timeout)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict) or "result" not in data or "resources" not in data["result"]:
            logging.error("Unexpected API response structure: %s", data)
            raise ValueError("API response does not contain 'result' or 'resources'.")

        # Extract filename and download url

        file_names = [(f["name"], f["url"]) for f in data["result"]["resources"] if f["url"].lower().endswith(".csv")]
        logging.info("Successfully fetched %d file(s) from API.", len(file_names))
        return file_names
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch file names and urls. Error: %s", e)
        raise


def fetch_oklahoma_gl_csv_from_url(file_info: Tuple[str, str], timeout: int = 60) -> Tuple[str, bytes]:
    """
    Fetches a General Ledger CSV file from the given URL.

    This function retrieves a CSV file from the Oklahoma Government website
    based on the provided file name and URL. The file content is returned
    as binary data along with the file name.

    :param file_info: A tuple containing the file name and the URL
    :type file_info: Tuple[str, str]
    :param timeout: Timeout settings for the HTTP request.
    :type timeout: int
    :return: A tuple containing the name of the fetched file and data as binary content
    :rtype: Tuple[str, bytes]
    """
    if not file_info:
        raise ValueError("file_info must be a tuple containing (file_name, file_url)")

    file_name, file_url = file_info

    try:
        response: Response = requests.get(url=file_url, stream=True, timeout=timeout)
        response.raise_for_status()
        logging.info("Successfully fetched file '%s'", file_name)
        return file_name, response.content
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch file '%s' from URL: %s. Error: %s", file_name, file_url, e)
        raise
