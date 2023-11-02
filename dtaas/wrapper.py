"""
Wrapper which does the following:

  1. Takes user SQL query and converts it to Mongo spec using custom sqlparse codebase (@lbabetto/sqlparse)
  2. Runs query and retrieves the matching files, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it locally, feeding the input files list to the `main` function
  4. Retrieves the output files from the `main` function and saves the files (of any kind, user-defined) in a zipped 
  archive which the user can then download

NOTE: User script must have a `main(input_files)` function which takes as argument a lists of paths with the
input files and returns a list of paths with the output/processed files.

Author: @lbabetto
"""

import os
import sys
from importlib import import_module
import shutil
from typing import List, Dict
from pymongo import MongoClient
from sqlparse.builders.mongo_builder import MongoQueryBuilder

from dtaas.utils import load_config, parse_cli_input

config = load_config()

import logging

logging.basicConfig(
    filename=config["LOGGING"]["logfile"],
    format=config["LOGGING"]["format"],
    level=config["LOGGING"]["level"].upper(),
    filemode=config["LOGGING"]["filemode"],
)

MONGODB_URI = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"
CLIENT = MongoClient(MONGODB_URI)
logging.info(f"Connected to client: {MONGODB_URI}")


def convert_SQL_to_mongo(sql_query: str) -> (Dict[str, str], Dict[str, str]):
    """Converts SQL query to MongoDB spec

    Parameters
    ----------
    sql_query : str
        SQL query

    Returns
    -------
    (Dict[str, str], Dict[str, str])
        dictionaries containing the filters (WHERE) and fields (SELECT) in MongoDB spec
    """
    builder = MongoQueryBuilder()
    logging.info(f"User query: {sql_query}")

    try:
        mongo_query = builder.parse_and_build(sql_query)
        query_filters = mongo_query[0]
        try:
            query_fields = mongo_query[1]["fields"]
        except KeyError:  # if no fields are present, parser does not add the key
            query_fields = {}
    except IndexError:
        logging.debug("Missing WHERE clause from SQL query, returning all data in the database.")
        query_fields = "{}"
        query_filters = "{}"
    logging.info(f"MongoDB query filter: {query_filters}")
    logging.info(f"MongoDB query fields: {query_fields}")
    return query_filters, query_fields


def retrieve_files(query_filters: Dict[str, str], query_fields: Dict[str, str]) -> List[str]:
    """Generate a file path list according to user query

    Parameters
    ----------
    query_filters : Dict[str, str]
        dictionary containing the query filters in MongoDB spec
    query_fields : Dict[str, str]
        dictionary containing the query fields in MongoDB spec

    Returns
    -------
    List[str]
        list containing the paths of the files matching the query
    """
    query_matches = []
    for entry in CLIENT[config["MONGO"]["database"]][config["MONGO"]["collection"]].find(query_filters, query_fields):
        query_matches.append(entry["path"])
    logging.debug(f"Query results: {query_matches}")
    return query_matches


def run_script(script: str, files_in: List[str]) -> List[str]:
    """Runs the `main` function in the user-provided Python script, feeding the paths containted in files_in.
    This function should take a list of paths as input and return a list of paths as output.

    Parameters
    ----------
    script : str
        Python script provided by the user, to be run on the query results
    files_in : List[str]
        list of path with the files on which to run the script

    Returns
    -------
    List[str]
        list of paths with the output/processed files the user wants to save

    Raises
    ------
    TypeError
        if the user-provided script `main` function does not return a list, abort the run
    """
    logging.info(f"User script: \n{script}")

    with open("user_script.py", "w") as f:
        f.write(script)
    if "user_script" in sys.modules:  # currently not needed, but if in future more than 1 script will be needed...
        del sys.modules["user_script"]
    user_module = import_module("user_script")

    user_main = getattr(user_module, "main")

    files_out = user_main(files_in)
    os.remove("user_script.py")
    if type(files_out) != list:
        raise TypeError("`main` function does not return a list of paths. ABORTING")

    return files_out


def save_output(files_out: List[str]):
    """_summary_

    Parameters
    ----------
    files_out : List[str]
        list containing the paths to the files to be saved
    """
    # TODO: make this consistent with S3 bucket implementation, right now only zips the files.
    # TODO: add try/except to make sure user returned the correct list of paths.

    logging.debug(f"Processed results: {files_out}")

    os.makedirs(f"RESULTS", exist_ok=True)
    for file in files_out:
        shutil.copy(file, f"RESULTS/{os.path.basename(file)}")
    shutil.make_archive("results", "zip", "RESULTS")
    shutil.rmtree("RESULTS")
    logging.info("Processed files available in the results.zip archive")


def run_wrapper(sql_query: str, script: str):
    """Get the SQL query and script, convert them to MongoDB spec, run the process query on the DB retrieving
    matching files, run the user-provided script (if present), retrieve the output file list from the main function,
    save the files and zip them in an archive

    Parameters
    ----------
    sql_query : str
        SQL query
    script : str
        Python script provided by the user, to be run on the query results
    """
    query_filters, query_fields = convert_SQL_to_mongo(sql_query=sql_query)
    files_in = retrieve_files(query_fields=query_fields, query_filters=query_filters)
    if script:
        files_out = run_script(script=script, files_in=files_in)
        save_output(files_out=files_out)
    else:
        save_output(files_out=files_in)


if __name__ == "__main__":
    sql_query, script = parse_cli_input()
    run_wrapper(sql_query=sql_query, script=script)
