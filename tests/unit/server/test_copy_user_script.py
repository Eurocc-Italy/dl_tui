import pytest

#
# Testing the copy_user_script function in module server.py
#

import os
import json
from dtaas.tuilib.common import Config
from dtaas.tuilib.server import create_remote_directory, copy_user_script


def test_copy_user_script(config_server: Config):
    """
    Create remote directory and copy user script file
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata",
                "script_path": "user_script.py",
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    create_remote_directory(json_path="input.json")
    stdout, stderr = copy_user_script(json_path="input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DTAAS-TUI-TEST/user_script.py'") == 0
    ), "File was not copied"

    assert (
        os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/DTAAS-TUI-TEST/user_script.py'").read()
        == "def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"
    )


def test_copy_user_script_full_path(config_server: Config):
    """
    Create remote directory and copy user script file from full path
    """

    config = config_server
    with open(f"{os.getcwd()}/input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata",
                "script_path": "user_script.py",
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    create_remote_directory(json_path=f"{os.getcwd()}/input.json")
    stdout, stderr = copy_user_script(json_path=f"{os.getcwd()}/input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DTAAS-TUI-TEST/user_script.py'") == 0
    ), "File was not copied"

    assert (
        os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/DTAAS-TUI-TEST/user_script.py'").read()
        == "def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"
    )


def test_no_script(config_server: Config):
    """
    Create remote directory and make sure nothing is copied if no script is found in input
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata",
            },
            f,
        )

    create_remote_directory(json_path="input.json")
    stdout, stderr = copy_user_script(json_path="input.json")

    assert stdout == ""
    assert stderr == ""

    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DTAAS-TUI-TEST/user_script.py'") == 512
    ), "Something was not copied"
