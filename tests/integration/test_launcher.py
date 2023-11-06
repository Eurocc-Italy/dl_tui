import pytest

#
# Testing the launcher module
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
from zipfile import ZipFile

from dtaas.launcher import


def test_just_search(test_collection, save_query):
    """
    Search for two specific files
    """

    save_query("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")

    os.system(f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000222564.jpg", "COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
