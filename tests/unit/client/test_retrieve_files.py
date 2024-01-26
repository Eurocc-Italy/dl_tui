import pytest

#
# Testing retrieve_files function in client.py library
#

from dtaas.tuilib.client import retrieve_files


def test_search_specific_files(mock_mongodb):
    """
    Search for two specific files
    """

    mongo_filters = {"$or": [{"id": 1}, {"id": 2}]}
    mongo_fields = {}

    files_in = retrieve_files(
        collection=mock_mongodb,
        query_filters=mongo_filters,
        query_fields=mongo_fields,
    )

    assert files_in == [
        "/test/path/testfile_1.txt",
        "/test/path/testfile_2.txt",
    ]


def test_search_missing_entry(mock_mongodb):
    """
    Search for a file that does not exist
    """

    mongo_filters = {"id": "PIPPO"}
    mongo_fields = {}

    files_in = retrieve_files(
        collection=mock_mongodb,
        query_filters=mongo_filters,
        query_fields=mongo_fields,
    )

    assert files_in == []
