import pytest
import os
import json

from pymongo import MongoClient
from dtaas.utils import load_config


@pytest.fixture(scope="session")
def test_collection():
    """Setting up testing environment and yielding test MongoDB collection

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    # creating custom config file
    with open(f"{os.path.dirname(os.path.abspath(__file__))}/../etc/config.json", "w") as f:
        json.dump({"MONGO": {"ip": "localhost"}}, f)

    # loading config and setting up
    config = load_config()
    mongodb_uri = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"

    # connecting to client
    client = MongoClient(mongodb_uri)

    # accessing collection
    collection = client[config["MONGO"]["database"]][config["MONGO"]["collection"]]

    # running tests
    yield collection

    # removing custom config file
    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/../etc/config.json")


@pytest.fixture(scope="function")
def save_query():
    def _save_query(sql_query):
        with open("QUERY", "w") as f:
            f.write(sql_query)

    yield _save_query
    os.remove("QUERY")


@pytest.fixture(scope="function")
def save_script():
    def _save_script(script_content):
        with open("script.py", "w") as f:
            f.write(script_content)

    yield _save_script
    os.remove("script.py")
