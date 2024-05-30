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
import json
from ast import literal_eval

import argparse

from dlaas.tuilib.common import Config
from dlaas.tuilib.api import upload, replace, update, download, delete, query, browse


def main():
    """API wrapper for the DLaaS TUI"""

    parser = argparse.ArgumentParser(
        description="""
Text User Interface for Cineca's Data Lake as a Service. 

For further information, please consult the code repository (https://gitlab.hpc.cineca.it/lbabetto/dlaas-tui).
""",
        epilog="""
Example commands [arguments within parentheses are optional]:

    UPLOAD   | dl_tui --upload --file=path/to/file.jpg --metadata=path/to/metadata.json
    REPLACE  | dl_tui --replace --file=path/to/file.jpg --metadata=path/to/metadata.json
    UPDATE   | dl_tui --update --key=file.jpg --metadata=path/to/metadata.json
    DOWNLOAD | dl_tui --download --key=file.jpg
    DELETE   | dl_tui --delete --key=file.jpg
    QUERY    | dl_tui --query --query_file=/path/to/query.txt [--python_file=/path/to/script.py] [--config_json=/path/to/config.json]
    BROWSE   | dl_tui --browse [--filter="category = dog"]
    """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Setting up actions
    actions = parser.add_mutually_exclusive_group()

    actions.add_argument(
        "--upload",
        help="upload a file and its metadata",
        action="store_true",
    )

    actions.add_argument(
        "--download",
        help="download a file",
        action="store_true",
    )

    actions.add_argument(
        "--delete",
        help="delete a file",
        action="store_true",
    )

    actions.add_argument(
        "--replace",
        help="replace a file and its metadata",
        action="store_true",
    )

    actions.add_argument(
        "--update",
        help="update the metadata of an entry",
        action="store_true",
    )

    actions.add_argument(
        "--browse",
        help="browse files in the Data Lake, optionally providing a filter",
        action="store_true",
    )

    actions.add_argument(
        "--query",
        help="launch a query, with an optional analysis script",
        action="store_true",
    )

    # Optional arguments

    parser.add_argument(
        "--ip",
        help="IP address of the server hosting the Data Lake API",
        default=Config("hpc").ip,
    )

    parser.add_argument(
        "--token",
        help="authentication token for launching commands to the Data Lake API",
        default=open(f"{os.environ['HOME']}/.config/dlaas/api-token.txt", "r").read(),
    )

    parser.add_argument(
        "--file",
        help="[--upload | --replace] | path to the file to be uploaded/replaced",
        default=None,
    )

    parser.add_argument(
        "--key",
        help="[--download | --delete | --update] | S3 key of the file to be downloaded/deleted/updated",
        default=None,
    )

    parser.add_argument(
        "--metadata",
        help="[--upload | --update | --replace] | path to the JSON file containing the metadata for the selected file",
        default=None,
    )

    parser.add_argument(
        "--query_file",
        help="[--query] | Path to the text file containing the SQL query to be run",
        default=None,
    )

    parser.add_argument(
        "--python_file",
        help="[--query] | path to the Python analysis script to be run on the files matching the query. \
        (--query only). Please see the User Guide for the script syntax requirements",
        default=None,
    )

    parser.add_argument(
        "--config_json",
        help="[--query] | path to the JSON file containing the custom configuration options. \
        Please see the User Guide for further details on how to customise analysis jobs",
        default=None,
    )

    parser.add_argument(
        "--filter",
        help="[--browse] | SQL-like query for filtering files, \
        where the 'SELECT * FROM metadata WHERE' part of the SQL query was removed",
        default=None,
    )

    # Parsing args
    args = parser.parse_args()

    # Printing selected action in logger
    for key, val in (args.__dict__).items():
        if val:
            logger.info(f"Selected action: {key}")
            break

    # Upload file
    if args.upload:

        # checking for missing options
        if not args.file:
            raise KeyError("Required argument is missing: --file")
        if not args.metadata:
            raise KeyError("Required argument is missing: --metadata")

        response = upload(
            ip=args.ip,
            token=args.token,
            file=args.file,
            json_data=args.metadata,
        )
        if response.status_code == 201:
            msg = f"Successfully uploaded file {args.file}."
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Replace file
    elif args.replace:

        # checking for missing options
        if not args.file:
            raise KeyError("Required argument is missing: --file")
        if not args.metadata:
            raise KeyError("Required argument is missing: --metadata")

        response = replace(
            ip=args.ip,
            token=args.token,
            file=args.file,
            json_data=args.metadata,
        )
        if response.status_code == 201:
            msg = f"Successfully replaced file {args.file}."
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Update file
    elif args.update:

        # checking for missing options
        if not args.key:
            raise KeyError("Required argument is missing: --key")
        if not args.metadata:
            raise KeyError("Required argument is missing: --metadata")

        response = update(
            ip=args.ip,
            token=args.token,
            file=args.key,
            json_data=args.metadata,
        )
        if response.status_code == 201:
            msg = f"Successfully updated metadata for file {args.key}."
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Download file
    elif args.download:

        # checking for missing options
        if not args.key:
            raise KeyError("Required argument is missing: --key")

        response = download(
            ip=args.ip,
            token=args.token,
            file=args.key,
        )
        if response.status_code == 200:
            msg = f"Successfully downloaded file {args.key}."
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Delete file
    elif args.delete:

        # checking for missing options
        if not args.key:
            raise KeyError("Required argument is missing: --key")

        response = delete(
            ip=args.ip,
            token=args.token,
            file=args.key,
        )
        if response.status_code == 200:
            msg = f"Successfully deleted file {args.key}."
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Run query
    elif args.query:

        # checking for missing options
        if not args.query_file:
            raise KeyError("Required argument is missing: --query_file")

        # loading default options
        config_json = {
            "config_hpc": Config("hpc").__dict__,
            "config_server": Config("server").__dict__,
        }

        # loading custom options
        try:
            with open(args.config_json, "r") as f:
                custom_config = json.load(f)
                for key in custom_config:
                    config_json[key].update(custom_config[key])
        except TypeError:
            logger.info("No custom configuration provided. Keeping defaults.")

        logger.debug(f"config_hpc: {config_json['config_hpc']}")
        logger.debug(f"config_server: {config_json['config_server']}")

        response = query(
            ip=args.ip,
            token=args.token,
            query_file=args.query_file,
            python_file=args.python_file,
            config_json=config_json,
        )

        if response.status_code == 200:
            if args.python_file:
                msg = (
                    f"Successfully launched analysis script {args.python_file} on query {open(args.query_file).read()}."
                )
            else:
                msg = f"Successfully launched query {open(args.query_file).read()}."
            logger.info(msg)
            print(msg)

            msg = f"Job ID: {response.text.replace('Files processed successfully, ID: ', '')}"
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Browse files
    elif args.browse:

        response = browse(
            ip=args.ip,
            token=args.token,
            filter=args.filter,
        )
        if response.status_code == 200:
            files = literal_eval(response.text)
            msg = f"Filter: {args.filter}\n"
            msg += f"Files:\n  - "
            msg += ("\n  - ").join(files)
            logger.info(msg)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()


if __name__ == "__main__":
    main()
