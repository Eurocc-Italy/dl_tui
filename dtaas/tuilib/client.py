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

import boto3


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
    """Generate a list of paths according to user query, interrogating the MongoDB database.
    NOTE: entries must have a "path" key and must be available at that path on the filesystem.

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

    NOTE: The function is intended to be run in a temporary directory, no cleanup is built-in!

    TODO?: parallelize execution of main function, splitting the file list across queries?
    Is it a good idea? Maybe the user script assumes data integrity...

    Parameters
    ----------
    script : str
        content of the Python script provided by the user, to be run on the query results
    files_in : List[str]
        list of paths with the files on which to run the script

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

    # converting to absolute paths (useful for save_output func)
    files_out = [os.path.abspath(file) for file in files_out]

    return files_out


def save_output(files_out: List[str], s3_endpoint_url: str, s3_bucket: str, job_id: str):
    """Take a list of paths and save the corresponding files in a zipped archive,
    which is then uploaded to an S3 bucket.

    Parameters
    ----------
    files_out : List[str]
        list containing the paths to the files to be saved
    s3_endpoint_url: str
        URL where the S3 bucket is located
    s3_bucket : str
        name of the S3 bucket in which the results need to be saved
    job_id : str
        unique job identifier, used to create the S3 object key

    """

    # NOTE: S3 credentials must be saved in ~/.aws/config file
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=s3_endpoint_url,
    )

    logger.debug(f"Processed results: {files_out}")

    os.makedirs(f"results", exist_ok=True)

    for file in files_out:
        try:
            # TODO: consider using the move function to save storage
            shutil.copy(file, f"results/{os.path.basename(file)}")
        except FileNotFoundError:
            logger.error(f"No such file or directory: '{file}'")

    shutil.make_archive(f"results", "zip", "results")

    response = s3.upload_file(
        Filename="results.zip",
        Bucket=s3_bucket,
        Key=f"results_{job_id}.zip",
    )
    logger.debug(f"S3 upload response: {response}")

    shutil.rmtree(f"results")

    # TODO: check if this is true
    logger.info(f"Processed files available at the following URL: {s3_endpoint_url}/{s3_bucket}/results_{job_id}.zip")


def wrapper(
    collection: Collection,
    sql_query: str,
    s3_endpoint_url: str,
    s3_bucket: str,
    job_id: str,
    script: str = None,
):
    """Get the SQL query and script, convert them to MongoDB spec, run the process query on the DB retrieving
    matching files, run the user-provided script (if present) in a temporary directory, retrieve the output
    file list from the main function, save the files and zip them in an archive

    Parameters
    ----------
    collection : Collection
        MongoDB collection on which to run the query
    sql_query : str
        SQL query
    s3_endpoint_url: str
        URL where the S3 bucket is located
    s3_bucket : str
        name of the S3 bucket in which the results need to be saved
    job_id : str
        unique job identifier, used to create the S3 object key
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
        # creating unique temporary directory
        tdir = mkdtemp(
            prefix="run_script_",
            suffix=None,
            dir=os.getcwd(),
        )
        # moving to temporary directory and working within the context manager
        with pushd(tdir):
            files_out = run_script(script=script, files_in=files_in)
        save_output(
            files_out=files_out,
            s3_endpoint_url=s3_endpoint_url,
            s3_bucket=s3_bucket,
            key=job_id,
        )

        shutil.rmtree(tdir)

    else:
        save_output(files_in)
