import pytest

#
# Testing wrapper function in wrapper.py
#

from dtaas.tuilib.client import wrapper
import os
from zipfile import ZipFile


def test_search_specific_files(test_collection):
    """
    Search for two specific files
    """
    query = "SELECT * FROM metadata WHERE id = 554625 OR id = 222564"

    wrapper(
        collection=test_collection,
        sql_query=query,
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == [
            "COCO_val2014_000000222564.jpg",
            "COCO_val2014_000000554625.jpg",
        ]

    os.remove("results.zip")


def test_search_specific_files_return_only_first(test_collection):
    """
    Search for two specific files and return just the first item
    """
    query = "SELECT * FROM metadata WHERE id = 554625 OR id = 222564"
    script = "def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"

    wrapper(
        collection=test_collection,
        sql_query=query,
        script=script,
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
