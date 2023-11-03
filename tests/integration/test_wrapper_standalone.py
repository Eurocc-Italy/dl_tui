import pytest

#
# Testing the wrapper module in standalone mode
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
from io import StringIO
from zipfile import ZipFile


def test_search_specific_files(test_collection):
    """
    Search for two specific files
    """

    sql_query = StringIO("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query '{sql_query.read()}'"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000222564.jpg", "COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_search_specific_files_reverse(test_collection):
    """
    Search for two specific files and only return the first item
    """

    sql_query = StringIO("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")
    script = StringIO("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query '{sql_query.read()}' --script '{script.read()}'"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_SQL(test_collection):
    """
    Search for a specific file using double quotes in SQL query
    """

    sql_query = StringIO('SELECT * FROM metadata WHERE captured = "2013-11-14 16:03:19"')

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query '{sql_query.read()}'"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_SQL(test_collection):
    """
    Search for a specific file using single quotes in SQL query and replacing them with double quotes.
    """

    sql_query = StringIO("SELECT * FROM metadata WHERE captured = '2013-11-14 16:03:19'")

    sql_query = sql_query.replace("'", '"')
    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query '{sql_query.read()}'"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_script(test_collection):
    """
    Search for a specific file using double quotes in user script
    """

    sql_query = StringIO("SELECT * FROM metadata WHERE id = 554625")
    script = StringIO('def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]')

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query '{sql_query.read()}' --script '{script.read()}'"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_script(test_collection):
    """
    Search for a specific file using single quotes in user script
    """

    sql_query = StringIO("SELECT * FROM metadata WHERE id = 554625")
    script = StringIO("def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]")

    os.system(
        f"python {os.path.dirname(os.path.abspath(__file__))}/../../dtaas/wrapper.py --query '{sql_query.read()}' --script '{script.read()}'"
    )

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
