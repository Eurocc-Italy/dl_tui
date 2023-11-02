import pytest

#
# Testing retrieve_files function in wrapper.py
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from dtaas.wrapper import convert_SQL_to_mongo, retrieve_files


def test_search_specific_files():
    """
    Search for two specific files
    """

    mongo_filter, mongo_fields = convert_SQL_to_mongo("""SELECT * FROM metadata WHERE id = 554625 OR id = 222564""")
    files_in = retrieve_files(query_filters=mongo_filter, query_fields=mongo_fields)
    assert files_in == [
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
    ]


def test_search_missing_entry():
    """
    Search for a file that does not exist
    """
    mongo_filter, mongo_fields = convert_SQL_to_mongo("""SELECT * FROM metadata WHERE id = PIPPO""")
    files_in = retrieve_files(query_filters=mongo_filter, query_fields=mongo_fields)
    assert files_in == []
