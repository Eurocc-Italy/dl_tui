from pymongo.collection import Collection
import pytest

#
# Testing the wrapper module in standalone mode
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
from zipfile import ZipFile
from dtaas.tuilib.common import sanitize_string


def test_search_only(test_collection: Collection):
    """
    Search for two specific files
    """

    query = "SELECT * FROM metadata WHERE id = 554625 OR id = 222564"

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += f'{{"ID": "42", "query": "{query}"}}'
    os.system(sanitize_string(version="client", string=cmd))

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == [
            "COCO_val2014_000000222564.jpg",
            "COCO_val2014_000000554625.jpg",
        ]

    os.remove("results.zip")


def test_return_first(test_collection: Collection):
    """
    Search for two specific files and only return the first item
    """

    query = "SELECT * FROM metadata WHERE id = 554625 OR id = 222564"
    script = r"def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += f'{{"ID": 42, "query": "{query}", "script": "{script}"}}'
    os.system(sanitize_string(version="client", string=cmd))

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


@pytest.mark.xfail  # TODO: consider converting all double quotes to single quotes
def test_double_quotes_in_SQL(test_collection: Collection):
    """
    Search for a specific file using double quotes in SQL query
    """

    query = 'SELECT * FROM metadata WHERE captured = "2013-11-14 16:03:19"'

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += f'{{"ID": 42, "query": "{query}"}}'
    os.system(sanitize_string(version="client", string=cmd))

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_SQL(test_collection: Collection):
    """
    Search for a specific file using single quotes in SQL query and replacing them with double quotes.
    """

    query = "SELECT * FROM metadata WHERE captured = '2013-11-14 16:03:19'"

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += f'{{"ID": 42, "query": "{query}"}}'
    os.system(sanitize_string(version="client", string=cmd))

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


@pytest.mark.xfail  # TODO: consider converting all double quotes to single quotes
def test_double_quotes_in_script(test_collection: Collection):
    """
    Search for a specific file using double quotes in user script
    """

    query = "SELECT * FROM metadata WHERE id = 554625"
    script = r"def main\(files_in\):\n files_out=files_in.copy\(\)\n print\(\"HELLO!\"\)\n return \[files_out\[0\]\]"

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += f'{{"ID": 42, "query": "{query}", "script": "{script}"}}'
    os.system(sanitize_string(version="client", string=cmd))

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_script(test_collection: Collection):
    """
    Search for a specific file using single quotes in user script
    """

    query = "SELECT * FROM metadata WHERE id = 554625"

    script = (
        r"def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]"
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf'{{"ID": 42, "query": "{query}", "script": "{script}"}}'

    os.system(sanitize_string(version="client", string=cmd))

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
