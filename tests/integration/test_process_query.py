import pytest
import os

#
# Testing that the queries are correctly processed in the database and the correct files are returned.
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from dtaas.wrapper import convert_SQL_to_mongo, process_query

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


# searching for two specific files
def test_search_and_return():
    try:
        mongo_filter, mongo_fields = convert_SQL_to_mongo("""SELECT * FROM metadata WHERE id = 554625 OR id = 222564""")
        files_in = process_query(query_filters=mongo_filter, query_fields=mongo_fields)
    except:
        assert False, "Unexpected exception."
    else:
        assert files_in == [
            "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
            "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
        ]
