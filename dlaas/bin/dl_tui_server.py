#!/usr/bin/env python
"""
Module to interface with HPC from the VM running the API server. The purpose of this module is to take the 
user query and script, launch a HPC job which calls the `dl_tui_hpc` executable on the compute nodes and 
runs the script on the query results.

Author: @lbabetto
"""

# setting up logging
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
if not os.path.exists("/var/log/datalake"):
    try:
        os.makedirs("/var/log/datalake")
    except PermissionError:
        fh = logging.FileHandler("dl-tui.log", mode="w")
else:
    fh = logging.FileHandler("/var/log/datalake/dl-tui.log", mode="a")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

import argparse
from dlaas.tuilib.server import (
    create_remote_directory,
    copy_json_input,
    copy_user_script,
    copy_user_container,
    launch_python,
    launch_container,
    upload_results,
)


def main():
    """Executable intended to run on the server VM"""

    parser = argparse.ArgumentParser(
        description="""
Server-side executable for Cineca's Data Lake as a Service.
        
For further information, please consult the code repository (https://github.com/Eurocc-Italy/dl_tui)
""",
        epilog="""
Example commands [arguments within parentheses are optional]:

    PYTHON SCRIPT           | dl_tui_server --python launch.json
    SINGULARITY CONTAINER   | dl_tui_server --container launch.json
    """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Setting up actions
    actions = parser.add_mutually_exclusive_group()

    actions.add_argument(
        "--python",
        help="launch a Python script on HPC",
        action="store_true",
    )

    actions.add_argument(
        "--container",
        help="launch a Singularity container on HPC",
        action="store_true",
    )

    parser.add_argument(
        "json_path",
        help="path to the JSON file containing the HPC job information",
    )

    args = parser.parse_args()
    json_path = args.json_path

    create_remote_directory(json_path=json_path)
    copy_json_input(json_path=json_path)

    if args.python:
        copy_user_script(json_path=json_path)
        stdout, stderr, slurm_job_id = launch_python(json_path=json_path)
    elif args.container:
        copy_user_container(json_path=json_path)
        stdout, stderr, slurm_job_id = launch_container(json_path=json_path)

    # FIXME uncomment for production
    # upload_results(json_path=json_path, slurm_job_id=slurm_job_id)


if __name__ == "__main__":
    main()
