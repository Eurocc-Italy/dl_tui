import pytest

import os
from dtaas.tuilib.common import Config
import shutil
from glob import glob
import mongomock
from pymongo import MongoClient
import boto3

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
    server = config_server
    os.system(f"ssh -i {server.ssh_key} {server.user}@{server.host} 'rm -rf ~/DTAAS-TUI-TEST-*'")


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


# @pytest.fixture(scope="function")
# def empty_bucket():
#     """Sets up an empty S3 bucket using the LocalStack local server

#     Yields
#     ------
#     boto3.Session.client
#         S3 client on which to run the tests
#     """
#     s3 = boto3.resource(
#         "s3",
#         endpoint_url="http://localhost.localstack.cloud:4566",
#         aws_access_key_id="test",
#         aws_secret_access_key="test",
#     )
#     s3.create_bucket(Bucket="emptybucket")
#     bucket = s3.Bucket("emptybucket")

#     yield bucket

#     bucket.objects.all().delete()


# @pytest.fixture(scope="function")
# def test_bucket():
#     """Sets up the test S3 bucket using the LocalStack local server

#     Yields
#     ------
#     boto3.Session.client
#         S3 client on which to run the tests
#     """
#     s3 = boto3.resource(
#         "s3",
#         endpoint_url="http://localhost.localstack.cloud:4566",
#         aws_access_key_id="test",
#         aws_secret_access_key="test",
#     )
#     s3.create_bucket(Bucket="testbucket")
#     bucket = s3.Bucket("testbucket")
#     bucket.upload_file(
#         Key="test1.txt",
#         Filename=f"{ROOT_DIR}/tests//utils/sample_files/test1.txt",
#     )
#     bucket.upload_file(
#         Key="test2.txt",
#         Filename=f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
#     )

#     yield bucket

#     bucket.objects.all().delete()
