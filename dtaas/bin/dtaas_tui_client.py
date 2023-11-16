#!/usr/bin/env python
"""
Wrapper which needs to be run from command line providing the following information in JSON format:

  - ID: a unique ID for the job run (preferably of the UUID.hex form)
  - query: path to a file containing an SQL query
  - script (optional): the content of a Python script. This Python script must contain a `main` function which takes 
    as input a list (of file paths), does some user-defined analysis, and returns a list (of file paths) which the 
    wrapper then takes and stores in a compressed archive. TODO: S3 implementation and POST to datalake.

The wrapper then does the following:

  1. Takes user SQL query and converts it to Mongo spec using a custom sqlparse codebase (@lbabetto/sqlparse)
  2. Runs query on remote DB and retrieves the matching entries, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it locally, feeding the input files list to the `main` function
  4. Retrieves the output files from the `main` function and saves the files (of any kind, user-defined) in a zipped 
  archive which the user can then download

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

from pymongo import MongoClient

import sys
from dtaas.tuilib.common import Config, UserInput
from dtaas.tuilib.client import wrapper


def main():
    """Client-side (HPC) version of the DTaaS TUI"""
    json_path = sys.argv[1]

    # reading user input
    user_input = UserInput.from_json(json_path=json_path)

    # loading config and overwriting custom options
    config = Config(version="client")
    if user_input.config_client:
        config.load_custom_config(user_input.config_client)
    logger.debug(config)

    # setting up MongoDB URI
    mongodb_uri = f"mongodb://{config.user}:{config.password}@{config.ip}:{config.port}/"

    # connecting to client
    logger.info(f"Connecting to client: {mongodb_uri}")
    client = MongoClient(mongodb_uri)

    # accessing collection
    logger.info(f"Loading database {config.database}, collection {config.collection}")
    collection = client[config.database][config.collection]

    if user_input.script_path:
        with open(user_input.script_path, "r") as f:
            script = f.read()
    else:
        script = None

    wrapper(
        collection=collection,
        sql_query=user_input.sql_query,
        script=script,
    )


if __name__ == "__main__":
    main()