import pytest

import os
import json
from pymongo import MongoClient
from dtaas.tuilib.common import Config
import shutil
from glob import glob


@pytest.fixture(scope="session", autouse=True)
def cleanup():
    client = Config(version="client")
    server = Config(version="server")
    yield
    for match in glob("run_script_*"):
        shutil.rmtree(match)
    if os.path.exists("client.log"):
        os.remove("client.log")
    if os.path.exists("server.log"):
        os.remove("server.log")

    os.system(f"ssh -i {server.ssh_key} {server.user}@{server.host} 'rm -rf ~/DTAAS-TUI-TEST-*'")


@pytest.fixture(scope="module")
def test_collection():
    """Setting up testing environment and yielding test MongoDB collection

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    # creating custom configuration files
    with open(f"{os.path.dirname(os.path.abspath(__file__))}/../dtaas/etc/config_client.json", "w") as f:
        json.dump({"ip": "localhost"}, f)

    with open(f"{os.path.dirname(os.path.abspath(__file__))}/../dtaas/etc/config_server.json", "w") as f:
        json.dump({"walltime": "00:10:00", "nodes": 1, "ntasks_per_node": 1}, f)

    # loading config and setting up testing environment
    config = Config(version="client")
    mongodb_uri = f"mongodb://{config.user}:{config.password}@{config.ip}:{config.port}/"

    # connecting to client
    client = MongoClient(mongodb_uri)

    # accessing collection
    collection = client[config.database][config.collection]

    # running tests
    yield collection

    # removing custom configuration files
    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/../dtaas/etc/config_client.json")
    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/../dtaas/etc/config_server.json")


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
