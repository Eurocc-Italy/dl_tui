import pytest

#
# Testing the copy_json_input function in module server.py
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
import json
from dtaas.tuilib.server import create_remote_directory, copy_json_input


def test_copy_json_input(config_server):
    """
    Create remote directory and copy json input file
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "ID": "DTAAS-TUI-TEST-copy_json",
                "sql_query": "SELECT * FROM metadata",
            },
            f,
        )
    create_remote_directory(json_path="input.json")
    stdout, stderr = copy_json_input(json_path="input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DTAAS-TUI-TEST-copy_json/input.json'")
        == 0
    ), "File was not copied"

    assert json.loads(
        os.popen(
            f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/DTAAS-TUI-TEST-copy_json/input.json'"
        ).read()
    ) == {
        "ID": "DTAAS-TUI-TEST-copy_json",
        "sql_query": "SELECT * FROM metadata",
    }
