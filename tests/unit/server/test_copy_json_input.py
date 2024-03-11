import pytest

#
# Testing the copy_json_input function in module server.py
#

import os
import json
from dlaas.tuilib.common import Config
from dlaas.tuilib.server import create_remote_directory, copy_json_input


def test_copy_json_input(config_server: Config):
    """
    Create remote directory and copy json input file
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata",
            },
            f,
        )
    create_remote_directory(json_path="input.json")
    stdout, stderr = copy_json_input(json_path="input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DLAAS-TUI-TEST/input.json'") == 0
    ), "File was not copied"

    assert json.loads(
        os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/DLAAS-TUI-TEST/input.json'").read()
    ) == {
        "id": "DLAAS-TUI-TEST",
        "sql_query": "SELECT * FROM metadata",
    }


def test_copy_json_from_full_path(config_server: Config):
    """
    Create remote directory and copy json input file from full path
    """

    config = config_server
    with open(f"{os.getcwd()}/input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata",
            },
            f,
        )
    create_remote_directory(json_path=f"{os.getcwd()}/input.json")
    stdout, stderr = copy_json_input(json_path=f"{os.getcwd()}/input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DLAAS-TUI-TEST/input.json'") == 0
    ), "File was not copied"

    assert json.loads(
        os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/DLAAS-TUI-TEST/input.json'").read()
    ) == {
        "id": "DLAAS-TUI-TEST",
        "sql_query": "SELECT * FROM metadata",
    }
