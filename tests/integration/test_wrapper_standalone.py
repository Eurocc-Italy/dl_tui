import pytest

#
# Testing the wrapper module in standalone mode
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
from io import StringIO
from zipfile import ZipFile


def test_search_specific_files(test_collection, save_query):
    """
    Search for two specific files
    """

    save_query("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")

    os.system(f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000222564.jpg", "COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_search_specific_files_reverse(test_collection, save_query, save_script):
    """
    Search for two specific files and only return the first item
    """

    save_query("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")
    save_script("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY --script script.py"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_SQL(test_collection, save_query):
    """
    Search for a specific file using double quotes in SQL query
    """

    save_query('SELECT * FROM metadata WHERE captured = "2013-11-14 16:03:19"')

    os.system(f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_SQL(test_collection, save_query):
    """
    Search for a specific file using single quotes in SQL query and replacing them with double quotes.
    """

    save_query("SELECT * FROM metadata WHERE captured = '2013-11-14 16:03:19'")

    os.system(f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_script(test_collection, save_query, save_script):
    """
    Search for a specific file using double quotes in user script
    """

    save_query("SELECT * FROM metadata WHERE id = 554625")
    save_script('def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]')

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY --script script.py"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_script(test_collection, save_query, save_script):
    """
    Search for a specific file using single quotes in user script
    """

    save_query("SELECT * FROM metadata WHERE id = 554625")
    save_script("def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]")

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query QUERY --script script.py"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
