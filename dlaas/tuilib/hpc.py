"""
Functions for HPC operations

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


def save_output(
    sql_query: str,
    script: str,
    files_out: List[str],
    pfs_prefix_path: str,
    s3_endpoint_url: str,
    s3_bucket: str,
    job_id: str,
    collection: Collection,
):
    """Take a list of paths and save the corresponding files in a zipped archive, updating the MongoDB database
    with the relevant data for the job (path oh parallel filesystem, s3 key, job identifier). Also, prepares a Python
    script for the upload of the results to the S3 bucket, which is supposed to be launched via a subsequent job
    on HPC with access to the S3 bucket.

    Parameters
    ----------
    sql_query : str
        SQL query (for saving in results folder)
    script : str
        content of the Python script provided by the user (for saving in results folder)
    files_out : List[str]
        list containing the paths to the files to be saved
    pfs_prefix_path : str
        path prefix for the location on the parallel filesystem
    s3_endpoint_url : str
        endpoint url at which the S3 bucket can be found
    s3_bucket : str
        name of the S3 bucket in which the results need to be saved
    job_id : str
        unique job identifier, used to create the S3 object key
    collection : Collection
        MongoDB collection on which to save the results metadata
    """

    logger.debug(f"Processed results: {files_out}")

    os.makedirs(f"results", exist_ok=True)

    # copying query and processing script to results folder
    with open(f"results/query_{job_id}.txt", "w") as f:
        f.write(sql_query)
    if script:
        with open(f"results/user_script_{job_id}.py", "w") as f:
            f.write(script)

    for file in files_out:
        try:
            # consider using shutil.move to save space, here and when copying the results
            shutil.copy(file, f"results/{os.path.basename(file)}")
        except FileNotFoundError:
            logger.error(f"No such file or directory: '{file}'")

    with open(f"upload_results_{job_id}.py", "w") as f:
        content = "import os, boto3, shutil, glob\n"
        content += 'for match in glob.glob("../slurm-*"):\n'
        content += ' shutil.copy(match, f"results/{os.path.basename(match)}")\n'
        content += f'shutil.make_archive("results_{job_id}", "zip", "results")\n'
        content += 'shutil.rmtree("results")\n'
        content += f's3 = boto3.client(service_name="s3", endpoint_url="{s3_endpoint_url}")\n'
        content += f's3.upload_file(Filename="results_{job_id}.zip", Bucket="{s3_bucket}", Key="results_{job_id}.zip")'
        f.write(content)

    collection.insert_one(
        {
            "job_id": job_id,
            "s3_key": f"results_{job_id}.zip",
            "path": f"{pfs_prefix_path}/results_{job_id}.zip",
        }
    )


def wrapper(
    collection: Collection,
    sql_query: str,
    pfs_prefix_path: str,
    s3_endpoint_url: str,
    s3_bucket: str,
    job_id: str,
    script: str = None,
):
    """Get the SQL query and script, convert them to MongoDB spec, run the process query on the DB retrieving
    matching files, run the user-provided script (if present) in a temporary directory, retrieve the output
    file list from the main function, save the files and zip them in an archive. This archive is then moved to
    the parallel filesystem at the location {pfs_prefix_path}/results_{job_id}.zip. Finally, the S3
    bucket is synced via curl and the results are uploaded to the MongoDB database with key results_{job_id}.zip

    Parameters
    ----------
    collection : Collection
        MongoDB collection on which to run the query and save the results metadata
    sql_query : str
        SQL query
    pfs_prefix_path: str
        path prefix for the location on the parallel filesystem
    s3_endpoint_url : str
        endpoint url at which the S3 bucket can be found
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
                sql_query=sql_query,
                script=script,
                files_out=files_out,
                pfs_prefix_path=pfs_prefix_path,
                s3_endpoint_url=s3_endpoint_url,
                s3_bucket=s3_bucket,
                job_id=job_id,
                collection=collection,
            )
    else:  # if no script is provided, return the query matches
        save_output(
            sql_query=sql_query,
            script=script,
            files_out=files_in,
            pfs_prefix_path=pfs_prefix_path,
            s3_endpoint_url=s3_endpoint_url,
            s3_bucket=s3_bucket,
            job_id=job_id,
            collection=collection,
        )
