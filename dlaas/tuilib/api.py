"""
Wrapper functions for API calls

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import requests
from requests import Response

import os
import json

from typing import Dict


def upload(
    ip: str,
    token: str,
    file: str,
    json_data: str,
) -> Response:
    """Upload file to Data Lake using the DLaaS API.

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

    response = requests.post(f"https://{ip}.nip.io/v1/upload", headers=headers, files=files)

    logger.info(f"Uploading file {file} to Data Lake. Response: {response.status_code}")

    return response


def replace(
    ip: str,
    token: str,
    file: str,
    json_data: str,
) -> Response:
    """Replace file in Data Lake using the DLaaS API.

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

    response = requests.put(f"https://{ip}.nip.io/v1/replace", headers=headers, files=files)

    logger.info(f"Replacing file {file} in Data Lake. Response: {response.status_code}")

    return response


def update(
    ip: str,
    token: str,
    file: str,
    json_data: str,
) -> Response:
    """Update file metadata in Data Lake using the DLaaS API.

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

    data = {
        "file": (file, file, "text/plain"),
    }

    files = {
        "json_data": (os.path.basename(json_data), open(json_data, "r"), "application/json"),
    }

    response = requests.patch(f"https://{ip}.nip.io/v1/update", headers=headers, data=data, files=files)

    logger.info(f"Updating metadata for file {file} in Data Lake. Response: {response.status_code}")

    return response


def download(
    ip: str,
    token: str,
    file: str,
) -> Response:
    """Download file from Data Lake using the DLaaS API.

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

    response = requests.get(f"https://{ip}.nip.io/v1/download", headers=headers, params={"file_name": file})

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
    """Delete file in Data Lake using the DLaaS API.

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

    response = requests.delete(f"https://{ip}.nip.io/v1/delete", headers=headers, params={"file_name": file})

    logger.info(f"Deleting file {file} from Data Lake. Response: {response.status_code}")

    return response


def query_python(
    ip: str,
    token: str,
    query_file: str,
    config_json: Dict[str, Dict[str, str]],
    python_file: str = None,
) -> Response:
    """Upload file to Data Lake using the DLaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    query_file : str
        Path of the file containing the SQL query to be launched
    config_json: Dict[str, Dict[str, str]]
        Dictionary containing the config_hpc and config_server configuration dictionaries
    python_file : str, optional
        Path of the Python analysis script to be ran on the query results

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    if python_file:
        files = {
            "query_file": (os.path.basename(query_file), open(query_file, "r"), "text/plain"),
            "python_file": (os.path.basename(python_file), open(python_file, "rb")),
        }
    else:
        files = {
            "query_file": (os.path.basename(query_file), open(query_file, "r"), "text/plain"),
        }

    response = requests.post(
        f"https://{ip}.nip.io/v1/query_and_process",
        headers=headers,
        files=files,
        data={"config_json": json.dumps(config_json)},
    )

    if python_file:
        logger.info(
            f"Running analysis script {python_file} on files matching query {query_file}. Response: {response.status_code}"
        )
    else:
        logger.info(f"Running query {query_file}. Response: {response.status_code}")

    return response


def query_container(
    ip: str,
    token: str,
    query_file: str,
    config_json: Dict[str, Dict[str, str]],
    container_path: str = None,
    container_url: str = None,
    exec_command: str = None,
) -> Response:
    """Upload file to Data Lake using the DLaaS API.

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    query_file : str
        Path of the file containing the SQL query to be launched
    config_json: Dict[str, Dict[str, str]]
        Dictionary containing the config_hpc and config_server configuration dictionaries
    container_path : str, optional
        Path to the Singularity container provided by the user
    container_url : str
        URL to the Docker/Singularity container provided by the user
    exec_command : str, optional
        Command to be launched within the container (with its own options and flags if needed)

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    if container_path:
        if container_url:
            raise KeyError(
                "Either provide the path to a local container or a URL to a pre-built one. Cannot process both."
            )
        files = {
            "query_file": (os.path.basename(query_file), open(query_file, "r"), "text/plain"),
            "container_file": (os.path.basename(container_path), open(container_path, "rb")),
        }
    else:
        files = {
            "query_file": (os.path.basename(query_file), open(query_file, "r"), "text/plain"),
        }

    response = requests.post(
        f"https://{ip}.nip.io/v1/launch_container",
        headers=headers,
        files=files,
        data={"config_json": json.dumps(config_json), "exec_command": exec_command, "container_url": container_url},
    )

    if container_path or container_url:
        logger.info(
            f"Running Singularity container {container_path or container_url} with command {exec_command} on files matching query {query_file}. Response: {response.status_code}"
        )
    else:
        logger.info(f"Running query {query_file}. Response: {response.status_code}")

    return response


def browse(
    ip: str,
    token: str,
    filter: str = None,
) -> Response:
    """Browse files in Data Lake, optionally setting SQL-like filters

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    filter : str, optional
        SQL query to filter the files

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    response = requests.get(f"https://{ip}.nip.io/v1/browse_files", headers=headers, params={"filter": filter})

    logger.info(f"Bwowsing files in from Data Lake. Filter: {filter}. Response: {response.status_code}")

    return response


def job_status(
    ip: str,
    token: str,
    user: str = None,
) -> Response:
    """Check HPC job status, optionally filtering by Data Lake user

    Parameters
    ----------
    ip : str
        IP address of the machine running the API
    token : str
        Authorization token for running commands via the API
    user : str, optional
        Data Lake user whose jobs you want to see

    Returns
    -------
    Response
        Response of the server request
    """

    token = token.rstrip("\n")
    headers = {
        "Authorization": f"Bearer {token}",
    }

    response = requests.get(f"https://{ip}.nip.io/v1/job_status", headers=headers, params={"user": user})

    logger.info(f"Checking job status on HPC. User: {filter}. Response: {response.status_code}")

    return response
