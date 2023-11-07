import pytest

import os
import json
from pymongo import MongoClient
from tuilib.common import Config


@pytest.fixture(scope="session")
def test_collection():
    """Setting up testing environment and yielding test MongoDB collection

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    # loading config and setting up testing environment
    with open(
        f"{os.path.dirname(os.path.abspath(__file__))}/../etc/config_client.json", "w"
    ) as f:
        json.dump({"ip": "localhost"}, f)

    config = Config(version="client")
    mongodb_uri = (
        f"mongodb://{config.user}:{config.password}@{config.ip}:{config.port}/"
    )

    # connecting to client
    client = MongoClient(mongodb_uri)

    # accessing collection
    collection = client[config.database][config.collection]

    # running tests
    yield collection

    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/../etc/config_client.json")


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
