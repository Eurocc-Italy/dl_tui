#!/usr/bin/env python
"""
Wrapper for the API calls, used to convert user input to CURL commands to be sent to the API

Author: @lbabetto
"""

# setting up logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("client.log", mode="w")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

import os
import sys

from dtaas.tuilib.common import Config
from dtaas.tuilib.api import upload, replace, update, download, delete, query


def main():
    """API wrapper for the DTaaS TUI"""

    allowed_requests = ["upload", "download", "delete", "replace", "update", "query"]

    try:
        request = sys.argv[1]
        user_input = sys.argv[2:]
    except IndexError:
        print("Empty request. Please consult the manual for how to use the DTaaS API wrapper.")
        return

    logger.info(f"User input for API call: {sys.argv[1:]}")

    if request.lower() not in allowed_requests:
        raise SyntaxError(f"Request '{request}' not allowed. Please use one of the following: {allowed_requests}")

    # Initializing options
    input_dict = {}
    options = [
        "ip",
        "token",
        "file",
        "json_data",
        "query_file",
        "python_file",
    ]

    # Loading default IP
    input_dict["ip"] = Config("client").ip

    # Loading default token
    try:
        with open(f"{os.environ['HOME']}/.config/dtaas-tui/api-token", "r") as f:
            input_dict["token"] = f.read()
    except FileNotFoundError:
        logger.info("Token not found. Unless provided explicitly via the token=... option, the commands will not work.")

    # Reading user-provided options
    for key in user_input:
        for option in options:
            if key.startswith(f"{option}="):
                input_dict[option] = key.lstrip(f"{option}=")
                break

    logger.debug(f"Options dictionary: {input_dict}")

    # Upload file
    if request.lower() == "upload":
        try:
            response = upload(
                ip=input_dict["ip"],
                token=input_dict["token"],
                file=input_dict["file"],
                json_data=input_dict["json_data"],
            )
            if response.status_code == 200:
                logger.info(f"Successfully uploaded file {input_dict['file']}.")
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Replace file
    if request.lower() == "replace":
        try:
            response = replace(
                ip=input_dict["ip"],
                token=input_dict["token"],
                file=input_dict["file"],
                json_data=input_dict["json_data"],
            )
            if response.status_code == 200:
                logger.info(f"Successfully replaced file {input_dict['file']}.")
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Update file
    if request.lower() == "update":
        try:
            response = update(
                ip=input_dict["ip"],
                token=input_dict["token"],
                file=input_dict["file"],
                json_data=input_dict["json_data"],
            )
            if response.status_code == 200:
                logger.info(f"Successfully updated metadata for file {input_dict['file']}.")
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Download file
    elif request.lower() == "download":
        try:
            response = download(
                ip=input_dict["ip"],
                token=input_dict["token"],
                file=input_dict["file"],
            )
            if response.status_code == 200:
                logger.info(f"Successfully downloaded file {input_dict['file']}.")
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Delete file
    elif request.lower() == "delete":
        try:
            response = delete(
                ip=input_dict["ip"],
                token=input_dict["token"],
                file=input_dict["file"],
            )
            if response.status_code == 200:
                logger.info(f"Successfully deleted file {input_dict['file']}.")
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Run query
    if request.lower() == "query":
        try:
            response = query(
                ip=input_dict["ip"],
                token=input_dict["token"],
                query_file=input_dict["query_file"],
                python_file=input_dict["python_file"],
            )
            if response.status_code == 200:
                logger.info(
                    f"Successfully launched analysis script {input_dict['python_file']} on query {input_dict['query_file']}."
                )
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")


if __name__ == "__main__":
    main()
