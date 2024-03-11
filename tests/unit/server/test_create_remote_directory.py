import pytest

#
# Testing the create_remote_directory function in module server.py
#

import os
import json
from dlaas.tuilib.common import Config
from dlaas.tuilib.server import create_remote_directory


def test_create_remote_directory(config_server: Config):
    """
    Create remote directory
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
    stdout, stderr = create_remote_directory(json_path="input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DLAAS-TUI-TEST'") == 0
    ), "Folder was not created"


def test_create_remote_directory_full_path(config_server: Config):
    """
    Create remote directory from full path in json input
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
    stdout, stderr = create_remote_directory(json_path=f"{os.getcwd()}/input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DLAAS-TUI-TEST'") == 0
    ), "Folder was not created"


def test_create_existing_directory():
    """
    Create remote directory which already exists
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata",
            },
            f,
        )

    # creating directory
    create_remote_directory(json_path="input.json")

    # trying to create same directory again
    with pytest.raises(RuntimeError):
        create_remote_directory(json_path="input.json")
