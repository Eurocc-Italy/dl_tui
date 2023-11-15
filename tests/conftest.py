import pytest

import os
from pymongo import MongoClient
from dtaas.tuilib.common import Config
import shutil
from glob import glob


@pytest.fixture(scope="module")
def config_client():
    config_client = Config("client")
    config_client.load_custom_config({"ip": "localhost"})
    return config_client


@pytest.fixture(scope="module")
def config_server():
    config_server = Config("server")
    config_server.load_custom_config({"walltime": "00:10:00", "ntasks_per_node": 1})
    return config_server


@pytest.fixture(scope="function", autouse=True)
def cleanup(config_server):
    # run test
    yield

    # cleanup temporary files and folders
    for match in glob("run_script*"):
        shutil.rmtree(match)
    if os.path.exists("user_script.py"):
        os.remove("user_script.py")
    if os.path.exists("input.json"):
        os.remove("input.json")
    if os.path.exists("client.log"):
        os.remove("client.log")
    if os.path.exists("server.log"):
        os.remove("server.log")

    # removing temporary folders on HPC
    server = config_server
    os.system(f"ssh -i {server.ssh_key} {server.user}@{server.host} 'rm -rf ~/DTAAS-TUI-TEST-*'")


@pytest.fixture(scope="module")
def test_collection(config_client):
    """Setting up testing environment and yielding test MongoDB collection

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    # load config and set up testing environment
    config = config_client
    mongodb_uri = f"mongodb://{config.user}:{config.password}@{config.ip}:{config.port}/"

    # connect to client
    client = MongoClient(mongodb_uri)

    # access collection
    collection = client[config.database][config.collection]

    # run tests
    yield collection


@pytest.fixture(scope="function")
def generate_test_files():
    """generate a text file to test the library

    Yields
    ------
    str
        path to the test file
    """
    with open("TESTFILE_1.txt", "w") as f:
        f.write("THIS IS THE FIRST TEST FILE.\n")
    with open("TESTFILE_2.txt", "w") as f:
        f.write("THIS IS THE SECOND TEST FILE.\n")

    yield ["TESTFILE_1.txt", "TESTFILE_2.txt"]

    os.remove("TESTFILE_1.txt")
    os.remove("TESTFILE_2.txt")
