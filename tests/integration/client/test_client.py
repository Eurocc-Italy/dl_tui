import pytest

#
# Testing the wrapper module in standalone mode
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
import json
from zipfile import ZipFile


def test_search_only():
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "42",
                "sql_query": "SELECT * FROM metadata WHERE id = 554625 OR id = 222564",
                "config_client": {"ip": "localhost"},
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == [
            "COCO_val2014_000000222564.jpg",
            "COCO_val2014_000000554625.jpg",
        ]

    os.remove("results.zip")


def test_return_first():
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "42",
                "sql_query": "SELECT * FROM metadata WHERE id = 554625 OR id = 222564",
                "script_path": "user_script.py",
                "config_client": {"ip": "localhost"},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_SQL(config_client):
    """
    Search for a specific file using double quotes in SQL query
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "42",
                "sql_query": 'SELECT * FROM metadata WHERE captured = "2013-11-14 16:03:19"',
                "config_client": {"ip": "localhost"},
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_SQL():
    """
    Search for a specific file using single quotes in SQL query.
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "42",
                "sql_query": "SELECT * FROM metadata WHERE captured = '2013-11-14 16:03:19'",
                "config_client": {"ip": "localhost"},
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_script():
    """
    Search for a specific file using double quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "42",
                "sql_query": "SELECT * FROM metadata WHERE id = 554625",
                "script_path": "user_script.py",
                "config_client": {"ip": "localhost"},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write('def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]')

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_script():
    """
    Search for a specific file using single quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "42",
                "sql_query": "SELECT * FROM metadata WHERE id = 554625",
                "script_path": "user_script.py",
                "config_client": {"ip": "localhost"},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]")

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
