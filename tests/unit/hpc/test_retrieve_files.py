import pytest

#
# Testing retrieve_files function in hpc.py library
#

from dlaas.tuilib.hpc import retrieve_files
from conftest import ROOT_DIR


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
        f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
        f"{ROOT_DIR}/tests/utils/sample_files/test2.txt",
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
