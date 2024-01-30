import pytest

import os
from dtaas.tuilib.common import Config
import shutil
from glob import glob
import mongomock
from pymongo import MongoClient

from os.path import dirname, abspath

ROOT_DIR = dirname(dirname(abspath(__file__)))  # points to the dtaas-tui directory


@pytest.fixture(scope="function", autouse=True)
def cleanup(config_server):
    # run test
    yield

    # cleanup temporary files and folders
    for match in glob("run_script*"):
        shutil.rmtree(match)
    for match in glob("results_*.zip"):
        os.remove(match)
    for match in glob("upload_results_*.py"):
        os.remove(match)
    if os.path.exists("user_script.py"):
        os.remove("user_script.py")
    if os.path.exists("input.json"):
        os.remove("input.json")
    if os.path.exists("client.log"):
        os.remove("client.log")
    if os.path.exists("server.log"):
        os.remove("server.log")

    # removing temporary folders on HPC
    os.system(f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'rm -rf ~/DTAAS-TUI-TEST-*'")


@pytest.fixture(scope="function")
def mock_mongodb():
    """Setting up a mock MongoDB collection

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    collection = mongomock.MongoClient().db.collection

    collection.insert_many(
        [
            {
                "id": 1,
                "s3_key": "test1.txt",
                "path": f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
            },
            {
                "id": 2,
                "s3_key": "test2.txt",
                "path": f"{ROOT_DIR}/tests/utils/sample_files/test2.txt",
            },
        ]
    )

    # run tests
    yield collection

    collection.delete_many({})


@pytest.fixture(scope="function")
def test_mongodb():
    """Setting up a test MongoDB collection

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    client = MongoClient("mongodb://user:passwd@localhost:27017/")
    collection = client["test_db"]["test_coll"]

    collection.insert_many(
        [
            {
                "id": 1,
                "s3_key": "test1.txt",
                "path": f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
            },
            {
                "id": 2,
                "s3_key": "test2.txt",
                "path": f"{ROOT_DIR}/tests/utils/sample_files/test2.txt",
            },
        ]
    )

    # run tests
    yield collection

    collection.delete_many({})


@pytest.fixture(scope="module")
def config_server():
    config_server = Config("server")
    config_server.load_custom_config(
        {
            "user": "lbabetto",
            "host": "login02-ext.g100.cineca.it",
            "venv_path": "~/virtualenvs/dtaas",
            "ssh_key": "~/.ssh/luca-g100",
            "compute_partition": "g100_usr_prod",
            "upload_partition": "g100_all_serial",
            "qos": "g100_qos_dbg",
            "account": "cin_staff",
            "mail": "NO",
            "walltime": "00:01:00",
            "nodes": 1,
            "ntasks_per_node": 1,
        }
    )
    return config_server


@pytest.fixture(scope="module")
def config_client():
    config_client = Config("client")
    config_client.load_custom_config(
        {
            "user": "user",
            "password": "passwd",
            "ip": "131.175.205.87",
            "port": "27017",
            "database": "datalake",
            "collection": "metadata",
            "s3_endpoint_url": "https://s3ds.g100st.cineca.it/",
            "s3_bucket": "s3poc",
            "pfs_prefix_path": "/g100_s3/DRES_",
        }
    )
    return config_client
