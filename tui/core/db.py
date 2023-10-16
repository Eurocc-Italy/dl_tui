"""
Functions and utilities to interface with the MongoDB database on the VM

!!! WIP !!!

Author: @lbabetto
"""

from pymongo import MongoClient
from hpc import copy_files, launch_job

from typing import Dict, Any
from pymongo.database import Database
from pymongo.collection import Collection

import logging

logger = logging.getLogger(__name__)

USER = "user"
PASSWORD = "passwd"
IP = "131.175.207.101"
PORT = "27017"

MONGODB_URI = f"mongodb://{USER}:{PASSWORD}@{IP}:{PORT}/"


def show_db_details(database: str):
    """Show database details (collections, ...)

    Parameters
    ----------
    database : str
        name of the database
    """
    client = MongoClient(MONGODB_URI)
    db = client[database]
    print(f"Database: {database}")
    print(f"  - Collections:")
    for collection in db.list_collections():
        print(f"    - {collection['name']}")


def filter(database: str, collection: str, filter_dict: Dict[str, Any]):
    """Filter database collection using a dictionary containing keys, value pairs

    Parameters
    ----------
    database : str
        MongoDB database containing the collection
    collection : str
        collection containing the data
    filter_dict : Dict[str, Any]
        dictionary containing the key, value pairs to use for filtering

    Returns
    -------
    list
        list containing the collection entries matching the filter
    """
    client = MongoClient(MONGODB_URI)
    coll = client[database][collection]

    filtered_list = []

    for entry in coll.find(filter_dict):
        filtered_list.append(entry)

    return filtered_list


def run_query(database: str, collection: str, query):
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
