import pytest

import os
import shutil
import json
from glob import glob
from ast import literal_eval

from dlaas.tuilib.common import Config
from dlaas.tuilib.api import upload, delete, browse

import mongomock
from pymongo import MongoClient

from os.path import dirname, abspath

ROOT_DIR = dirname(dirname(abspath(__file__)))  # points to the dlaas-tui repo directory


@pytest.fixture(scope="function")
def setup_testfiles_HPC():

    # creating test files
    with open("test1.txt", "w") as f:
        f.write("test1")
    with open("test2.txt", "w") as f:
        f.write("test1")

    # creating metadata for test files
    with open("test1.json", "w") as f:
        json.dump({"id": "1"}, f)
    with open("test2.json", "w") as f:
        json.dump({"id": "2"}, f)

    # loading IP and token for API
    ip = Config("hpc").ip
    with open(f"{os.environ['HOME']}/.config/dlaas/api-token.txt", "r") as f:
        token = f.read()

    # uploading test files to datalake via API
    upload(ip=ip, token=token, file="test1.txt", json_data="test1.json")
    upload(ip=ip, token=token, file="test2.txt", json_data="test2.json")

    # run tests
    yield

    # delete files from datalake via API
    delete(ip=ip, token=token, file="test1.txt")
    delete(ip=ip, token=token, file="test2.txt")

    # delete local test files
    os.remove("test1.txt")
    os.remove("test2.txt")
    os.remove("test1.json")
    os.remove("test2.json")


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
    if os.path.exists("hpc.log"):
        os.remove("hpc.log")
    if os.path.exists("server.log"):
        os.remove("server.log")
    if os.path.exists("tui.log"):
        os.remove("tui.log")

    # removing temporary folder on HPC
    os.system(f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'rm -rf ~/DLAAS-TUI-TEST'")

    # setting IP and token for cleanup
    ip = Config("hpc").ip
    with open(f"{os.environ['HOME']}/.config/dlaas/api-token.txt", "r") as f:
        token = f.read()

    # remove result files from datalake
    delete(ip=ip, token=token, file="results_DLAAS-TUI-TEST.zip")
    results = [entry for entry in literal_eval(browse(ip=ip, token=token).text) if "results" in entry]
    for result in results:
        delete(ip=ip, token=token, file=result)


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
                "id": "1",
                "s3_key": "test1.txt",
                "path": f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
            },
            {
                "id": "2",
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
    mongo = MongoClient("mongodb://user:passwd@localhost:27017/")
    collection = mongo["test_db"]["test_coll"]

    collection.insert_many(
        [
            {
                "id": "1",
                "s3_key": "test1.txt",
                "path": f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
            },
            {
                "id": "2",
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
            "venv_path": "~/dtaas_venv",
            "ssh_key": "~/.ssh/luca-hpc",
            "compute_partition": "g100_usr_dbg",
            "upload_partition": "g100_all_serial",
            "account": "EUCC_staff_2",
            "qos": "normal",
            "mail": "Nope",
            "walltime": "00:05:00",
            "nodes": 1,
            "ntasks_per_node": 1,
        }
    )
    return config_server


@pytest.fixture(scope="module")
def config_hpc():
    config_hpc = Config("hpc")
    config_hpc.load_custom_config(
        {
            "user": "user",
            "password": "passwd",
            "ip": "131.175.205.87",
            "port": "27017",
            "database": "datalake",
            "collection": "metadata",
            "s3_endpoint_url": "https://s3ds.g100st.cineca.it/",
            "s3_bucket": "s3poc",
            "pfs_prefix_path": "/g100_s3/DRES_s3poc",
        }
    )
    return config_hpc
