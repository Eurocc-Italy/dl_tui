"""
Functions for client-side (HPC) operations

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import os
import sys
import shutil
from sh import pushd
from tempfile import mkdtemp

from typing import List, Dict, Tuple
from pymongo.collection import Collection

from importlib import import_module
from sqlparse.builders.mongo_builder import MongoQueryBuilder


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

    mongo_query = builder.parse_and_build(query_string=sql_query)
    query_filters = mongo_query[0]

    try:
        query_fields = mongo_query[1]["fields"]
    except KeyError:  # if no fields are present, parser does not add the key
        query_fields = {}

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

    for entry in collection.find(filter=query_filters, projection=query_fields):
        query_matches.append(entry["path"])

    logger.debug(f"Query results: {query_matches}")

    return query_matches


def run_script(script: str, files_in: List[str]) -> List[str]:
    """Runs the `main` function in the user-provided Python script, feeding the paths containted in files_in.
    This function must take a list (of file paths) as input and return a list (of file paths) as output.

    TODO: parallelize execution of main function, splitting the file list across queries?
    Is it a good idea? Maybe the user script assumes data integrity...

    Parameters
    ----------
    script : str
        content of the Python script provided by the user, to be run on the query results
    files_in : List[str]
        list of path with the files on which to run the script

    Returns
    -------
    List[str]
        list of paths with the output/processed files the user wants to save

    Raises
    ------
    AttributeError
        if the user provides a script without a `main` function
    TypeError
        if the user-provided script `main` function does not return a list, abort the run
    """

    logger.info(f"User script:\n{script}")

    # creating unique temporary directory
    tdir = mkdtemp(
        prefix="run_script_",
        suffix=None,
        dir=os.getcwd(),
    )

    # moving to temporary directory and working within the context manager
    with pushd(tdir):
        with open("user_script.py", "w") as f:
            f.write(script)

        # loading user script dynamically
        sys.path.insert(0, os.getcwd())
        user_module = import_module("user_script")

        try:
            user_main = getattr(user_module, "main")
        except AttributeError:
            raise AttributeError(f"User-provided script has no `main` function")
        else:
            files_out = user_main(files_in)
        finally:
            del sys.modules["user_script"]

        if type(files_out) != list:
            raise TypeError("`main` function does not return a list of paths. ABORTING")

        shutil.rmtree(tdir)

    return files_out


def save_output(files_out: List[str]):
    """Take a list of paths and save the corresponding files in a zipped archive.

    Parameters
    ----------
    files_out : List[str]
        list containing the paths to the files to be saved
    """
    # TODO: make this consistent with S3 bucket implementation, right now only zips the files.

    logger.debug(f"Processed results: {files_out}")

    os.makedirs(f"RESULTS", exist_ok=True)

    for file in files_out:
        try:
            shutil.copy(file, f"RESULTS/{os.path.basename(file)}")
        except FileNotFoundError:
            logger.error(f"No such file or directory: '{file}'")

    shutil.make_archive("results", "zip", "RESULTS")
    shutil.rmtree("RESULTS")

    logger.info("Processed files available in the results.zip archive")


def wrapper(
    collection: Collection,
    sql_query: str,
    script: str = None,
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
    script : str, optional
        content of the Python script provided by the user, to be run on the query results
    """

    query_filters, query_fields = convert_SQL_to_mongo(sql_query=sql_query)

    files_in = retrieve_files(
        collection=collection,
        query_filters=query_filters,
        query_fields=query_fields,
    )

    if script:
        files_out = run_script(script=script, files_in=files_in)
        save_output(files_out)
    else:
        save_output(files_in)
