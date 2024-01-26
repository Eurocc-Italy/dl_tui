import pytest

#
# Testing retrieve_files function in client.py library
#

from dtaas.tuilib.client import retrieve_files


def test_search_specific_files(test_collection):
    """
    Search for two specific files
    """

    mongo_filters = {"$or": [{"id": 1}, {"id": 2}]}
    mongo_fields = {}

    files_in = retrieve_files(
        collection=test_collection,
        query_filters=mongo_filters,
        query_fields=mongo_fields,
    )

    assert files_in == [
        "/test/path/testfile_1.txt",
        "/test/path/testfile_2.txt",
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
