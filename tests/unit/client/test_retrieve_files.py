import pytest

#
# Testing retrieve_files function in client.py library
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from tuilib.client import retrieve_files


def test_search_specific_files(test_collection):
    """
    Search for two specific files
    """

    mongo_filters = {"$or": [{"id": 554625}, {"id": 222564}]}
    mongo_fields = {}

    files_in = retrieve_files(
        collection=test_collection,
        query_filters=mongo_filters,
        query_fields=mongo_fields,
    )

    assert files_in == [
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
    ]


def test_search_missing_entry(test_collection):
    """
    Search for a file that does not exist
    """

    mongo_filters = {"id": "PIPPO"}
    mongo_fields = {}

    files_in = retrieve_files(
        collection=test_collection,
        query_filters=mongo_filters,
        query_fields=mongo_fields,
    )

    assert files_in == []
