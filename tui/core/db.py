"""
Functions and utilities to interface with the MongoDB database on the VM

!!! WIP !!!

Author: @lbabetto
"""

from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

USER = "user"
PASSWORD = "passwd"
IP = "131.175.207.101"
PORT = "27017"

MONGODB_URI = f"mongodb://{USER}:{PASSWORD}@{IP}:{PORT}/"


def connect_to_db(db_name: str, collection_name: str):
    """Connect to MongoDB database <db_name> and retrieve document <collection_name>

    Parameters
    ----------
    db_name : str
        name of the MongoDB database
    collection_name : str
        name of the collection of interest

    Returns
    -------
    dict
        collection in form of json-style dictionary
    """
    client = MongoClient(MONGODB_URI)
    db = client[db_name]
    collection = db[collection_name]

    return collection


def run_query(db, query):
    """Wraps the user query function and runs it on the database

    Parameters
    ----------
    db : str
        database collection on which to run the query
    query : function
        user function to be wrapped and ran on the database
    """
    db = connect_to_db(db_name="COCO", collection_name="COCO")
    query(db)
