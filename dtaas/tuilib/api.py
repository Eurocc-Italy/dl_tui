"""
Wrapper functions for API calls

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import requests
from requests import Response

import os


def upload(
    ip: str,
    token: str,
    file: str,
    json_data: str,
) -> Response:
    """Upload file to Data Lake using the DTaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    file : str
        Path of the file to be uploaded
    json_data : str
        Path of the JSON file containing the metadata of the file to be uploaded

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    files = {
        "file": (os.path.basename(file), open(file, "rb"), None),
        "json_data": (os.path.basename(json_data), open(json_data, "r"), "application/json"),
    }

    response = requests.post(f"http://{ip}:8080/v1/upload", headers=headers, files=files)

    logger.info(f"Uploading file {file} to Data Lake. Response: {response.status_code}")

    return response


def replace(
    ip: str,
    token: str,
    file: str,
    json_data: str,
) -> Response:
    """Replace file in Data Lake using the DTaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    file : str
        Path of the file to be uploaded
    json_data : str
        Path of the JSON file containing the metadata of the file to be uploaded

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    files = {
        "file": (os.path.basename(file), open(file, "rb"), None),
        "json_data": (os.path.basename(json_data), open(json_data, "r"), "application/json"),
    }

    response = requests.put(f"http://{ip}:8080/v1/replace", headers=headers, files=files)

    logger.info(f"Replacing file {file} in Data Lake. Response: {response.status_code}")

    return response


def update(
    ip: str,
    token: str,
    file: str,
    json_data: str,
) -> Response:
    """Update file metadata in Data Lake using the DTaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    file : str
        Name of the file to be updated
    json_data : str
        Path of the JSON file containing the metadata of the file to be uploaded

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    files = {
        "file": (file, file, "text/plain"),
        "json_data": (os.path.basename(json_data), open(json_data, "r"), "application/json"),
    }

    response = requests.patch(f"http://{ip}:8080/v1/update", headers=headers, files=files)

    logger.info(f"Updating metadata for file {file} in Data Lake. Response: {response.status_code}")

    return response


def download(
    ip: str,
    token: str,
    file: str,
) -> Response:
    """Download file from Data Lake using the DTaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    file : str
        File to be downloaded

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "accept": "application/octet-stream",
        "Authorization": f"Bearer {token}",
    }

    response = requests.get(f"http://{ip}:8080/v1/download", headers=headers, params={"file_name": file})

    logger.info(f"Downloading file {file} from Data Lake. Response: {response.status_code}")

    if response.status_code == 200:
        with open(file, "wb") as f:
            f.write(response.content)

    return response


def delete(
    ip: str,
    token: str,
    file: str,
) -> Response:
    """Delete file in Data Lake using the DTaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    file : str
        File to be deleted

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    response = requests.delete(f"http://{ip}:8080/v1/delete", headers=headers, params={"file_name": file})

    logger.info(f"Deleting file {file} from Data Lake. Response: {response.status_code}")

    return response
