import pytest

#
# Testing the launcher module
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from tuilib.common import Config
from tuilib.server import launch_job


def test_just_search(test_collection, save_query):
    """
    Search for two specific files
    """
    import os

    os.system("hpc_credential_setup")

    save_query("SELECT * FROM metadata WHERE id = 554625 OR id = 222564")

    stdout, stderr = launch_job(
        config=load_config(), query_path="QUERY", script_path=None
    )

    assert stdout[:19] == "Submitted batch job"
    assert (
        stderr[:74]
        == "Warning: No xauth data; using fake authentication data for X11 forwarding."
    )
