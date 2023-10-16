"""
Functions and utilities to interface with the MongoDB database on the VM

!!! WIP !!!

Author: @lbabetto
"""

from typing import Callable, Any

from pymongo import MongoClient
from hpc import copy_files, launch_job

import logging

logger = logging.getLogger(__name__)

USER = "user"
PASSWORD = "passwd"
IP = "131.175.207.101"
PORT = "27017"

MONGODB_URI = f"mongodb://{USER}:{PASSWORD}@{IP}:{PORT}/"


def show_db_details(database: str):
    client = MongoClient(MONGODB_URI)
    db = client[database]
    print(f"Database: {database}")
    print(f"  collections:")
    for collection in db:
        print(f"    - {collection}")


def run_query(database: str, collection: str, query: Callable):
    """Wraps the user query function and runs it on the database

    Parameters
    ----------
    db : str
        database containing the collection of interest
    collection : str
        database collection on which to run the query
    query : Callable
        user function to be wrapped and run on the database
    """
    client = MongoClient(MONGODB_URI)
    db = client[database]
    data = db[collection]
    query(data)
