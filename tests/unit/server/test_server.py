import pytest

#
# Testing the launcher module
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from tuilib.common import Config, UserInput
from tuilib.server import launch_job


def test_just_search(test_collection):
    """
    Search for two specific files
    """
    import os

    os.system("hpc_credential_setup")

    config = Config("server")
    user_input = UserInput(
        {
            "ID": 42,
            "query": "SELECT * FROM metadata WHERE id = 554625 OR id = 222564",
        }
    )
    stdout, stderr = launch_job(config=config, user_input=user_input)

    assert stdout[:19] == "Submitted batch job"
