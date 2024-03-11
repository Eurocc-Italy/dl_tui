import pytest

import os
import json
from dlaas.tuilib.common import Config
from zipfile import ZipFile

from conftest import ROOT_DIR


#
# Testing the wrapper module in standalone mode
#
"""
NOTE: to be able to run correctly, a MongoDB database must be running, it must have a "user" user with 
    password "passwd" with access to the "test_db" database and "test_coll" collection, 
    in which these entries are stored. To do so, follow these steps:
    # TODO: automate also user creation via pymongo
        - Make sure the mongodb server is running and log in via command line (mongo or mongosh)
        - Switch to the admin database:
            use admin
        - Create the user:
            db.createUser(
                {
                    user: "user", 
                    pwd: "passwd", 
                    roles: [
                        {
                            role: "readWrite", 
                            db: "test_db"
                        }
                    ]
                }
            )
        - database and collection creation is taken care of by the fixtures
"""


@pytest.fixture(scope="function")
def config_hpc():
    config_hpc = Config("hpc")
    config_hpc.load_custom_config(
        {
            "user": "user",
            "password": "passwd",
            "ip": "localhost",
            "port": "27017",
            "database": "test_db",
            "collection": "test_coll",
            "s3_endpoint_url": "https://testurl.com/",
            "s3_bucket": "test",
            "pfs_prefix_path": ROOT_DIR,
        }
    )
    return config_hpc


def test_search_only(test_mongodb, config_hpc):
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": 1,
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    os.system(f"{ROOT_DIR}/dtaas/bin/dl_tui_hpc.py input.json")

    assert os.path.exists(f"results_1.zip"), "Zipped archive was not created."

    with ZipFile("results_1.zip", "r") as archive:
        assert archive.namelist() == [
            "test1.txt",
            "test2.txt",
        ], "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_1.py"), "Upload script was not created."

    with open("upload_results_1.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_1.zip", Bucket="test", Key="results_1.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in test_mongodb.find({"job_id": 1})]) == 1
    assert test_mongodb.find_one({"job_id": 1})["path"] == f"{ROOT_DIR}/results_1.zip"
    assert test_mongodb.find_one({"job_id": 1})["s3_key"] == "results_1.zip"


def test_return_first(test_mongodb, config_hpc):
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": 2,
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "script_path": "user_script.py",
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{ROOT_DIR}/dtaas/bin/dl_tui_hpc.py input.json")

    assert os.path.exists(f"results_2.zip"), "Zipped archive was not created."

    with ZipFile("results_2.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"], "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_2.py"), "Upload script was not created."

    with open("upload_results_2.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_2.zip", Bucket="test", Key="results_2.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in test_mongodb.find({"job_id": 2})]) == 1
    assert test_mongodb.find_one({"job_id": 2})["path"] == f"{ROOT_DIR}/results_2.zip"
    assert test_mongodb.find_one({"job_id": 2})["s3_key"] == "results_2.zip"


def test_double_quotes_in_SQL(test_mongodb, config_hpc):
    """
    Search for a specific file using double quotes in SQL query
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": 3,
                "sql_query": 'SELECT * FROM test_coll WHERE s3_key = "test1.txt"',
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    os.system(f"{ROOT_DIR}/dtaas/bin/dl_tui_hpc.py input.json")

    assert os.path.exists(f"results_3.zip"), "Zipped archive was not created."

    with ZipFile("results_3.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"], "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_3.py"), "Upload script was not created."

    with open("upload_results_3.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_3.zip", Bucket="test", Key="results_3.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in test_mongodb.find({"job_id": 3})]) == 1
    assert test_mongodb.find_one({"job_id": 3})["path"] == f"{ROOT_DIR}/results_3.zip"
    assert test_mongodb.find_one({"job_id": 3})["s3_key"] == "results_3.zip"


def test_single_quotes_in_SQL(test_mongodb, config_hpc):
    """
    Search for a specific file using single quotes in SQL query.
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": 4,
                "sql_query": "SELECT * FROM test_coll WHERE s3_key = 'test1.txt'",
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    os.system(f"{ROOT_DIR}/dtaas/bin/dl_tui_hpc.py input.json")

    assert os.path.exists(f"results_4.zip"), "Zipped archive was not created."

    with ZipFile("results_4.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"], "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_4.py"), "Upload script was not created."

    with open("upload_results_4.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_4.zip", Bucket="test", Key="results_4.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in test_mongodb.find({"job_id": 4})]) == 1
    assert test_mongodb.find_one({"job_id": 4})["path"] == f"{ROOT_DIR}/results_4.zip"
    assert test_mongodb.find_one({"job_id": 4})["s3_key"] == "results_4.zip"


def test_double_quotes_in_script(test_mongodb, config_hpc):
    """
    Search for a specific file using double quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": 5,
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "script_path": "user_script.py",
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write('def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]')

    os.system(f"{ROOT_DIR}/dtaas/bin/dl_tui_hpc.py input.json")

    assert os.path.exists(f"results_5.zip"), "Zipped archive was not created."

    with ZipFile("results_5.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"], "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_5.py"), "Upload script was not created."

    with open("upload_results_5.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_5.zip", Bucket="test", Key="results_5.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in test_mongodb.find({"job_id": 5})]) == 1
    assert test_mongodb.find_one({"job_id": 5})["path"] == f"{ROOT_DIR}/results_5.zip"
    assert test_mongodb.find_one({"job_id": 5})["s3_key"] == "results_5.zip"


def test_single_quotes_in_script(test_mongodb, config_hpc):
    """
    Search for a specific file using single quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": 6,
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "script_path": "user_script.py",
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]")

    os.system(f"{ROOT_DIR}/dtaas/bin/dl_tui_hpc.py input.json")

    assert os.path.exists(f"results_6.zip"), "Zipped archive was not created."

    with ZipFile("results_6.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"], "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_6.py"), "Upload script was not created."

    with open("upload_results_6.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_6.zip", Bucket="test", Key="results_6.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in test_mongodb.find({"job_id": 6})]) == 1
    assert test_mongodb.find_one({"job_id": 6})["path"] == f"{ROOT_DIR}/results_6.zip"
    assert test_mongodb.find_one({"job_id": 6})["s3_key"] == "results_6.zip"
