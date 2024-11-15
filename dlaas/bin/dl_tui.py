#!/usr/bin/env python
"""
Wrapper for the DLaaS API calls, used to convert user input to CURL commands to be sent to the API

Author: @lbabetto
"""

# setting up logging
import logging

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("> %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

import os
import json

import argparse

from dlaas.tuilib.common import Config
from dlaas.tuilib.api import (
    upload,
    replace,
    update,
    download,
    delete,
    query_python,
    query_container,
    browse,
    job_status,
)


def main():
    """API wrapper for the DLaaS TUI"""

    parser = argparse.ArgumentParser(
        description="""
Text User Interface for Cineca's Data Lake as a Service. 

For further information, please consult the code repository (https://github.com/Eurocc-Italy/dl_tui).
""",
        epilog="""
Example commands [arguments within parentheses are optional]:

    UPLOAD      | dl_tui --upload --file=path/to/file.jpg --metadata=path/to/metadata.json
    REPLACE     | dl_tui --replace --file=path/to/file.jpg --metadata=path/to/metadata.json
    UPDATE      | dl_tui --update --key=file.jpg --metadata=path/to/metadata.json
    DOWNLOAD    | dl_tui --download --key=file.jpg
    DELETE      | dl_tui --delete --key=file.jpg
    BROWSE      | dl_tui --browse [--filter="category = dog"]
    JOB_STATUS  | dl_tui --job_status [--user="john"] [--config_json=/path/to/config.json]
    QUERY (PYTHON)    | dl_tui --query --query_file=/path/to/query.txt [--python_file=/path/to/script.py] [--config_json=/path/to/config.json]
    QUERY (CONTAINER) | dl_tui --query --query_file=/path/to/query.txt [--container_path=/path/to/container.sif] [--container_url=docker://url/to/container.sif] [--exec_command="command to be executed within the container"] [--config_json=/path/to/config.json]
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
        "--job_status",
        help="check job status on HPC, optionally filtering for specific user",
        action="store_true",
    )

    actions.add_argument(
        "--query",
        help="launch a query, with an optional Python analysis script or Docker/Singularity container",
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
        "--container_file",
        help="[--query] | path to the Singularity container to be run on the files matching the query. \
        (--query only). Please see the User Guide for the script syntax requirements",
        default=None,
    )

    parser.add_argument(
        "--container_url",
        help="[--query] | URL to the Docker/Singularity container to be run on the files matching the query. \
        (--query only). Please see the User Guide for the script syntax requirements",
        default=None,
    )

    parser.add_argument(
        "--exec_command",
        help="[--query] | command to be executed within the Singularity container. \
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

    parser.add_argument(
        "--user",
        help="[--job_status] | Data Lake user for which to filter HPC jobs",
        default=None,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="sets verbosity of output (in order: WARNING (default) | INFO | DEBUG).",
    )

    # Parsing args
    args = parser.parse_args()

    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    if args.verbose > 1:
        logger.setLevel(logging.DEBUG)

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
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Run query
    elif args.query:

        # Check for missing query file
        if not args.query_file:
            raise KeyError("Required argument is missing: --query_file")

        if args.container_file or args.container_url:
            # Check that user did not give both Python and container options
            if args.python_file:
                raise KeyError(
                    "Analysis with both a Python script and a Singularity container were requested. The two modes are mutually exclusive."
                )
            # Check that execution command was provided
            if not args.exec_command:
                raise KeyError("Required argument is missing: --exec_command")

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

        if args.python_file:
            response = query_python(
                ip=args.ip,
                token=args.token,
                query_file=args.query_file,
                python_file=args.python_file,
                config_json=config_json,
            )

            if response.status_code == 200:
                msg = (
                    f"Successfully launched analysis script {args.python_file} on query {open(args.query_file).read()}"
                )
                print(msg)

                msg = f"Job ID: {response.text.replace('Files processed successfully, ID: ', '')}"
                print(msg)
            else:
                print(response.text)
                response.raise_for_status()

        else:
            response = query_container(
                ip=args.ip,
                token=args.token,
                query_file=args.query_file,
                container_path=args.container_file,
                container_url=args.container_url,
                exec_command=args.exec_command,
                config_json=config_json,
            )

            if response.status_code == 200:
                if args.container_file or args.container_url:
                    msg = rf"Successfully launched Singularity container {args.container_file or args.container_url} with command {args.exec_command} on query {open(args.query_file).read()}"
                else:
                    msg = f"Successfully launched query {open(args.query_file).read()}."
                print(msg)

                msg = f"Job ID: {response.text.replace('Files processed successfully, ID: ', '')}"
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
            files = json.loads(response.text)["files"]
            msg = f"Filter: {args.filter}\n"
            msg += f"Files:\n  - "
            msg += ("\n  - ").join(files)
            print(msg)
        else:
            print(response.text)
            response.raise_for_status()

    # Check job status
    elif args.job_status:

        response = job_status(
            ip=args.ip,
            token=args.token,
            user=args.user,
        )

        status_dict = {
            "BF": "BOOT_FAIL",
            "CA": "CANCELLED",
            "CD": "COMPLETED",
            "CF": "CONFIGURING",
            "CG": "COMPLETING",
            "DL": "DEADLINE",
            "F": "FAILED",
            "NF": "NODE_FAIL",
            "OOM": "OUT_OF_MEMORY",
            "PD": "PENDING",
            "PR": "PREEMPTED",
            "R": "RUNNING",
            "RD": "RESV_DEL_HOLD",
            "RF": "REQUEUE_FED",
            "RH": "REQUEUE_HOLD",
            "RQ": "REQUEUED",
            "RS": "RESIZING",
            "RV": "REVOKED",
            "SI": "SIGNALING",
            "SE": "SPECIAL_EXIT",
            "SO": "STAGE_OUT",
            "ST": "STOPPED",
            "S": "SUSPENDED",
            "TO": "TIMEOUT",
        }

        if response.status_code == 200:
            jobs = json.loads(response.text)["jobs"]
            print(f"{'JOB ID':^20} | {'SLURM JOB':^15} | {'STATUS':^14} | {'REASON'}")

            if jobs:
                for jobid, job in jobs.items():
                    print(
                        f"{job['DATA_LAKE_JOBID']:^20} | \
                        {job['JOBID']:^15} | \
                        {status_dict[job['ST']]:^14} | \
                        {job['REASON'] if job['REASON'] != 'None' else ''}"
                    )
        else:
            print(response.text)
            response.raise_for_status()


if __name__ == "__main__":
    main()
