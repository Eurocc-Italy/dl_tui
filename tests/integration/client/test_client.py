import pytest

#
# Testing the wrapper module in standalone mode
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
from zipfile import ZipFile


def process_string(string: str) -> str:
    """Takes a string and provides a shell-friendly version with properly escaped special characters

    Parameters
    ----------
    string : str
        string to be parsed

    Returns
    -------
    str
        parsed string
    """
    # TODO: implement full list of special characters
    return (
        string.replace("'", r"\'")
        .replace('"', r"\"")
        .replace("\n", r"\\n")
        .replace("*", r"\*")
        .replace("(", r"\(")
        .replace(")", r"\)")
    )


def test_search_only(test_collection):
    """
    Search for two specific files
    """

    query = process_string("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf"{{\"ID\": 42, \"query\": \"{query}\"}}"
    os.system(cmd)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == [
            "COCO_val2014_000000222564.jpg",
            "COCO_val2014_000000554625.jpg",
        ]

    os.remove("results.zip")


def test_return_reverse(test_collection):
    """
    Search for two specific files and only return the first item
    """

    query = process_string("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")
    script = process_string(
        "def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf"{{\"ID\": 42, \"query\": \"{query}\", \"script\": \"{script}\"}}"
    os.system(cmd)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


@pytest.mark.xfail  # TODO: consider converting all double quotes to single quotes
def test_double_quotes_in_SQL(test_collection):
    """
    Search for a specific file using double quotes in SQL query
    """

    query = process_string(
        'SELECT * FROM metadata WHERE captured = "2013-11-14 16:03:19"'
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf"{{\"ID\": 42, \"query\": \"{query}\"}}"
    os.system(cmd)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_SQL(test_collection):
    """
    Search for a specific file using single quotes in SQL query and replacing them with double quotes.
    """

    query = process_string(
        "SELECT * FROM metadata WHERE captured = '2013-11-14 16:03:19'"
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf"{{\"ID\": 42, \"query\": \"{query}\"}}"
    os.system(cmd)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


@pytest.mark.xfail  # TODO: consider converting all double quotes to single quotes
def test_double_quotes_in_script(test_collection):
    """
    Search for a specific file using double quotes in user script
    """

    query = process_string("SELECT * FROM metadata WHERE id = 554625")
    script = process_string(
        'def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]'
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf"{{\"ID\": 42, \"query\": \"{query}\", \"script\": \"{script}\"}}"
    os.system(cmd)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_script(test_collection):
    """
    Search for a specific file using single quotes in user script
    """

    query = process_string("SELECT * FROM metadata WHERE id = 554625")
    script = process_string(
        "def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]"
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client "
    cmd += rf"{{\"ID\": 42, \"query\": \"{query}\", \"script\": \"{script}\"}}"
    os.system(cmd)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
