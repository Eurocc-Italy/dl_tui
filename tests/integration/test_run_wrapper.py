import pytest

#
# Testing run_wrapper function in wrapper.py
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from dtaas.wrapper import run_wrapper
import os
from zipfile import ZipFile


def test_search_specific_files(test_collection):
    """
    Search for two specific files
    """
    run_wrapper(
        collection=test_collection,
        sql_query="""SELECT * FROM metadata WHERE id = 554625 OR id = 222564""",
        script="""def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out""",
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000222564.jpg", "COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
