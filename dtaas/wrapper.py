"""
Wrapper which needs to be run providing a --query flag from command line with a SQL query and (optionally) a 
--script flag with a Python script in text form. Ideally both flag arguments should be encased in triple quotes

The wrapper then does the following:

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
from typing import List, Dict, Tuple
from pymongo import MongoClient
from pymongo.collection import Collection
from sqlparse.builders.mongo_builder import MongoQueryBuilder

from dtaas.utils import load_config, parse_cli_input

import logging

logger = logging.getLogger(__name__)


def convert_SQL_to_mongo(sql_query: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Converts SQL query to MongoDB spec

    Parameters
    ----------
    sql_query : str
        SQL query

    Returns
    -------
    Tuple[Dict[str, str], Dict[str, str]]
        dictionaries containing the filters (WHERE) and fields (SELECT) in MongoDB spec
    """
    builder = MongoQueryBuilder()
    logger.info(f"User query: {sql_query}")

    try:
        mongo_query = builder.parse_and_build(sql_query)
        query_filters = mongo_query[0]
        try:
            query_fields = mongo_query[1]["fields"]
        except KeyError:  # if no fields are present, parser does not add the key
            query_fields = {}
    except IndexError:
        logger.debug("Missing WHERE clause from SQL query, returning all data in the database.")
        query_fields = "{}"
        query_filters = "{}"
    logger.info(f"MongoDB query filter: {query_filters}")
    logger.info(f"MongoDB query fields: {query_fields}")
    return query_filters, query_fields


def retrieve_files(
    collection: Collection,
    query_filters: Dict[str, str],
    query_fields: Dict[str, str],
) -> List[str]:
    """Generate a file path list according to user query

    Parameters
    ----------
    collection : Collection
        MongoDB collection on which to run the query
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
    for entry in collection.find(query_filters, query_fields):
        query_matches.append(entry["path"])
    logger.debug(f"Query results: {query_matches}")
    return query_matches


def run_script(script_path: str, files_in: List[str]) -> List[str]:
    """Runs the `main` function in the user-provided Python script, feeding the paths containted in files_in.
    This function should take a list of paths as input and return a list of paths as output.

    Parameters
    ----------
    script_file : str
        path to the Python script provided by the user, to be run on the query results
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
    logger.info(f"User script: {script_path}")

    module_name = os.path.splitext(script_path)[0]
    if module_name in sys.modules:
        raise NameError(
            f"Cannot use {module_name}.py as file name, as it conflicts with another currently loaded Python library"
        )

    # loading user script dynamically
    sys.path.insert(0, os.getcwd())
    user_module = import_module(module_name)

    # checking if main function is present
    try:
        user_main = getattr(user_module, "main")
    except AttributeError:
        del sys.modules[module_name]
        raise AttributeError(f"{script_path} has no `main` function")

    # running the main function, retrieving output files and cleaning up
    files_out = user_main(files_in)
    del sys.modules[module_name]

    if type(files_out) != list:
        raise TypeError("`main` function does not return a list of paths. ABORTING")

    return files_out


def save_output(files_out: List[str]):
    """Take a list of paths and save the corresponding files in a zipped archive.

    Parameters
    ----------
    files_out : List[str]
        list containing the paths to the files to be saved
    """
    # TODO: make this consistent with S3 bucket implementation, right now only zips the files.
    # TODO: add try/except to make sure user returned the correct list of paths.

    logger.debug(f"Processed results: {files_out}")

    os.makedirs(f"RESULTS", exist_ok=True)
    for file in files_out:
        shutil.copy(file, f"RESULTS/{os.path.basename(file)}")
    shutil.make_archive("results", "zip", "RESULTS")
    shutil.rmtree("RESULTS")
    logger.info("Processed files available in the results.zip archive")


def run_wrapper(
    collection: Collection,
    sql_query: str,
    script_path: str = None,
):
    """Get the SQL query and script, convert them to MongoDB spec, run the process query on the DB retrieving
    matching files, run the user-provided script (if present), retrieve the output file list from the main function,
    save the files and zip them in an archive

    Parameters
    ----------
    collection : Collection
        MongoDB collection on which to run the query
    sql_query : str
        SQL query
    script_path : str, optional
        path to the Python script provided by the user, to be run on the query results
    """
    query_filters, query_fields = convert_SQL_to_mongo(sql_query=sql_query)
    files_in = retrieve_files(
        collection=collection,
        query_fields=query_fields,
        query_filters=query_filters,
    )
    if script_path:
        files_out = run_script(script_path=script_path, files_in=files_in)
        save_output(files_out=files_out)
    else:
        save_output(files_out=files_in)


if __name__ == "__main__":
    # loading config and setting up URI
    config = load_config()
    mongodb_uri = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"

    # connecting to client
    logger.info(f"Connecting to client: {mongodb_uri}")
    client = MongoClient(mongodb_uri)

    # accessing collection
    logger.info(f"Loading database {config['MONGO']['database']}, collection {config['MONGO']['collection']}")
    collection = client[config["MONGO"]["database"]][config["MONGO"]["collection"]]

    # running query and script
    query_path, script_path = parse_cli_input()
    with open(query_path) as query_file:
        sql_query = query_file.read()
        run_wrapper(
            collection=collection,
            sql_query=sql_query,
            script_path=script_path,
        )
