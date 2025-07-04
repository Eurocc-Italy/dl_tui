#!/usr/bin/env python
"""
Wrapper which needs to be run from command line providing the following information in JSON format:

  - ID: a unique ID for the job run (preferably of the UUID.hex form)
  - query: path to a file containing an SQL query
  - script (optional): the content of a Python script. This Python script must contain a `main` function which takes
    as input a list (of file paths), does some user-defined analysis, and returns a list (of file paths) which the
    wrapper then takes and stores in a compressed archive.

The wrapper then does the following:

  1. Takes user SQL query and converts it to Mongo spec using a custom sqlparse codebase (@lbabetto/sqlparse)
  2. Runs query on remote DB and retrieves the matching entries, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it locally, feeding the input files list to the `main` function
  4. Retrieves the output files from the `main` function and saves the files (of any kind, user-defined) in a zipped
  archive which is then uploaded to the S3 bucket specified in the hpc config file.

Author: @lbabetto
"""

# setting up logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("dl-tui.log", mode="w")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

from pymongo import MongoClient

import argparse
from dlaas.tuilib.common import Config, UserInput
from dlaas.tuilib.hpc import python_wrapper, container_wrapper


def main():
    """Executable intended to run on HPC"""

    parser = argparse.ArgumentParser(
        description="""
HPC-side executable for Cineca's Data Lake as a Service.
        
For further information, please consult the code repository (https://github.com/Eurocc-Italy/dl_tui)
""",
    )

    parser.add_argument(
        "json_path",
        help="path to the JSON file containing the HPC job information",
    )

    args = parser.parse_args()
    json_path = args.json_path

    # reading user input
    user_input = UserInput.from_json(json_path=json_path)

    # loading config and overwriting custom options
    config = Config(version="hpc")
    if user_input.config_hpc:
        config.load_custom_config(user_input.config_hpc)
    logger.debug(f"HPC config: {config}")

    # setting up MongoDB URI
    mongodb_uri = f"mongodb://{config.user}:{config.password}@{config.ip}:{config.port}/"

    # connecting to MongoDB server
    logger.info(f"Connecting to MongoDB client: {mongodb_uri}")
    client = MongoClient(mongodb_uri)

    # accessing collection
    logger.info(f"Loading database {config.database}, collection {config.collection}")
    collection = client[config.database][config.collection]

    # Launch Singularity container (with path)
    if user_input.container_path:
        container_wrapper(
            collection=collection,
            sql_query=user_input.sql_query,
            pfs_prefix_path=config.pfs_prefix_path,
            s3_endpoint_url=config.s3_endpoint_url,
            s3_bucket=config.s3_bucket,
            job_id=user_input.id,
            container_path=user_input.container_path,
            exec_command=user_input.exec_command,
        )

    # Launch Singularity container (with URL)
    elif user_input.container_url:
        container_wrapper(
            collection=collection,
            sql_query=user_input.sql_query,
            pfs_prefix_path=config.pfs_prefix_path,
            s3_endpoint_url=config.s3_endpoint_url,
            s3_bucket=config.s3_bucket,
            job_id=user_input.id,
            container_path=f"container_{user_input.id}.sif",
            exec_command=user_input.exec_command,
        )

    # Launch Python script (if missing, should just return the query matches)
    else:
        if user_input.script_path:
            with open(user_input.script_path, "r") as f:
                script = f.read()

        python_wrapper(
            collection=collection,
            sql_query=user_input.sql_query,
            pfs_prefix_path=config.pfs_prefix_path,
            s3_endpoint_url=config.s3_endpoint_url,
            s3_bucket=config.s3_bucket,
            job_id=user_input.id,
            script=script,
        )


if __name__ == "__main__":
    main()
