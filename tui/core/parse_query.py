"""
Functions to parse the SQL query from the user and convert it to pymongo commands

!!! WIP - WILL PROBABLY NEED TO BE DELETED !!!

Author: @lbabetto
"""

import logging
from typing import Dict
from pyparsing import *

logger = logging.getLogger(__name__)


def convert_query(sql_query: str, db: Dict):
    """Converts SQL query into pymongo query

    Parameters
    ----------
    sql_query : str
        SQL query to be converted
    db : pymongo.database.Database
        MongoDB database on which to run the query

    Returns
    -------
    _type_
        _description_
    """

    query = sql_query.split()

    # SQL SELECT statement
    if query.pop(0).upper() == "SELECT":
        collection = None  # FROM statement
        fields = []  # SELECT statement
        filters = []  # WHERE statement

        from_id = None  # position of FROM statement
        where_id = None  # position of WHERE statement

        for id, token in enumerate(query):
            if token.upper() == "FROM":
                from_id = id
            elif token.upper() == "WHERE":
                where_id = id

        for token in query[0:from_id]:
            if token == "*":
                break
            fields.append(token.rstrip(","))

        if not from_id:
            raise SyntaxError(f"Missing FROM statement in SQL query '{sql_query}'.")
        collection = query[from_id + 1]

        if where_id:
            buffer = {}
            for token in query[where_id + 1 :]:
                if token.upper() == "AND":
                    filters.append(buffer)
                if token.upper() == "OR":
                    filters.append(buffer)

                if token == "=":
                    pass

                else:
                    buffer[token] = None

        print(f"collection: {collection}")
        print(f"fields: {fields}")
        print(f"filters: {filters}")
        func = db[collection].find(
            {},
            dict(zip(fields, [1 for field in fields])),
        )

    else:
        raise SyntaxError(f"Cannot parse SQL query '{sql_query}'")


convert_query('"SELECT name, age FROM people WHERE status = "A" AND age <= 50', None)
