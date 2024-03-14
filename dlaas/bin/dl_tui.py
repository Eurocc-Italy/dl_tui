#!/usr/bin/env python
"""
Wrapper for the DLaaS API calls, used to convert user input to CURL commands to be sent to the API

Author: @lbabetto
"""

# setting up logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("tui.log", mode="w")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

import os
import sys
import json

from dlaas.tuilib.common import Config
from dlaas.tuilib.api import upload, replace, update, download, delete, query, browse


def main():
    """API wrapper for the DLaaS TUI"""

    allowed_requests = [
        "upload",
        "download",
        "delete",
        "replace",
        "update",
        "query",
        "browse",
    ]

    try:
        request = sys.argv[1]
        user_input = sys.argv[2:]
    except IndexError:
        print("Empty request. Please consult the manual for how to use the DLaaS TUI.")
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
        "config_json",
        "filter",
    ]

    # Loading default IP
    input_dict["ip"] = Config("hpc").ip

    # Loading default token
    try:
        with open(f"{os.environ['HOME']}/.config/dlaas/api-token", "r") as f:
            input_dict["token"] = f.read()
    except FileNotFoundError:
        logger.info("Token not found. Unless provided explicitly via the token=... option, the commands will not work.")

    # Reading user-provided options
    for key in user_input:
        for option in options:
            if key.startswith(f"{option}="):
                input_dict[option] = key.replace(f"{option}=", "")
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
            if response.status_code == 201:
                msg = f"Successfully uploaded file {input_dict['file']}."
                logger.info(msg)
                print(msg)
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
            if response.status_code == 201:
                msg = f"Successfully replaced file {input_dict['file']}."
                logger.info(msg)
                print(msg)
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
            if response.status_code == 201:
                msg = f"Successfully updated metadata for file {input_dict['file']}."
                logger.info(msg)
                print(msg)
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
                msg = f"Successfully downloaded file {input_dict['file']}."
                logger.info(msg)
                print(msg)
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
                msg = f"Successfully deleted file {input_dict['file']}."
                logger.info(msg)
                print(msg)
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Run query
    if request.lower() == "query":

        config_json = {
            "config_hpc": Config("hpc").__dict__,
            "config_server": Config("server").__dict__,
        }

        try:
            with open(input_dict["config_json"], "r") as f:
                custom_config = json.load(f)
                for key in custom_config:
                    config_json[key].update(custom_config[key])
        except KeyError:
            logger.info("No custom configuration provided. Keeping defaults.")

        logger.debug(f"config_hpc: {config_json['config_hpc']}")
        logger.debug(f"config_server: {config_json['config_server']}")

        try:
            response = query(
                ip=input_dict["ip"],
                token=input_dict["token"],
                query_file=input_dict["query_file"],
                python_file=input_dict["python_file"],
                config_json=config_json,
            )
            if response.status_code == 200:
                msg = f"Successfully launched analysis script {input_dict['python_file']} on query {input_dict['query_file']}."
                logger.info(msg)
                print(msg)

                msg = f"Job ID: {response.text.replace('Files processed successfully, ID: ', '')}"
                logger.info(msg)
                print(msg)
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")

    # Browse files
    elif request.lower() == "browse":

        # Reading optional filter keyword
        try:
            filter = input_dict["filter"]
        except KeyError:
            filter = None

        try:
            response = browse(
                ip=input_dict["ip"],
                token=input_dict["token"],
                filter=filter,
            )
            if response.status_code == 200:
                files = eval(response.text)
                msg = f"Filter: {filter}\n"
                msg += f"Files:\n  - "
                msg += ("\n  - ").join(files)
                logger.info(msg)
                print(msg)
            else:
                print(response.text)
                response.raise_for_status()
        except KeyError as key:
            print(f"Required key is missing: {key}")


if __name__ == "__main__":
    main()
