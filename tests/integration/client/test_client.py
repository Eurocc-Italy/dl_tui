import pytest

import os
import shutil
import json
from dtaas.tuilib.common import Config
from zipfile import ZipFile


#
# Testing the wrapper module in standalone mode
#
"""
NOTE: to be able to run correctly, the following requsites must be met:

    1. A MongoDB database must be running, it must have a "user" user with password "passwd" 
    with access to the "test_db" database and "test_coll" collection, in which these entries are stored. 
    To do so, follow these steps:
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
        
    2. An S3 server (we recommend setting one up locally via localstack in a docker container) containing the
    corresponding S3 keys in a test bucket (test1.txt and test2.txt). To generate the files on the spot, follow
    these instructions:
    # TODO: automate docker setup and cleanup
        - Move to the dtaas-tui/tests/utils/localstack folder and start the docker environment: 
            docker-compose up -d
        - bucket and data setup is taken care of by the conftest fixtures
"""


@pytest.fixture(scope="function", autouse=True)
def create_tmpdir():
    """Creates temporary directory for storing results file, and then deletes it"""
    os.makedirs(f"{os.path.dirname(os.path.abspath(__file__))}/testbucket", exist_ok=True)
    yield
    shutil.rmtree(f"{os.path.dirname(os.path.abspath(__file__))}/testbucket")


@pytest.fixture(scope="function")
def config_client():
    config_client = Config("client")
    config_client.load_custom_config(
        {
            "user": "user",
            "password": "passwd",
            "ip": "localhost",
            "port": "27017",
            "database": "test_db",
            "collection": "test_coll",
            "s3_endpoint_url": "http://localhost.localstack.cloud:4566/",
            "s3_bucket": "testbucket",
            "pfs_prefix_path": f"{os.path.dirname(os.path.abspath(__file__))}/",
        }
    )
    return config_client


def test_search_only(test_bucket, test_mongodb, config_client):
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "1",
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "config_client": config_client.__dict__,
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/testbucket/results_1.zip"
    ), "Zipped archive was not created."

    test_bucket.download_file(Key="results_1.zip", Filename="results_1.zip")
    with ZipFile("results_1.zip", "r") as archive:
        assert archive.namelist() == [
            "test1.txt",
            "test2.txt",
        ]


def test_return_first(test_bucket, test_mongodb, config_client):
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "2",
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "script_path": "user_script.py",
                "config_client": config_client.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/testbucket/results_2.zip"
    ), "Zipped archive was not created."

    test_bucket.download_file(Key="results_2.zip", Filename="results_2.zip")
    with ZipFile("results_2.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"]


def test_double_quotes_in_SQL(test_bucket, test_mongodb, config_client):
    """
    Search for a specific file using double quotes in SQL query
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "3",
                "sql_query": 'SELECT * FROM test_coll WHERE s3_key = "test1.txt"',
                "config_client": config_client.__dict__,
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/testbucket/results_3.zip"
    ), "Zipped archive was not created."

    test_bucket.download_file(Key="results_3.zip", Filename="results_3.zip")
    with ZipFile("results_3.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"]


def test_single_quotes_in_SQL(test_bucket, test_mongodb, config_client):
    """
    Search for a specific file using single quotes in SQL query.
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "4",
                "sql_query": "SELECT * FROM test_coll WHERE s3_key = 'test1.txt'",
                "config_client": config_client.__dict__,
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/testbucket/results_4.zip"
    ), "Zipped archive was not created."

    test_bucket.download_file(Key="results_4.zip", Filename="results_4.zip")
    with ZipFile("results_4.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"]


def test_double_quotes_in_script(test_bucket, test_mongodb, config_client):
    """
    Search for a specific file using double quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "5",
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "script_path": "user_script.py",
                "config_client": config_client.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write('def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]')

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/testbucket/results_5.zip"
    ), "Zipped archive was not created."

    test_bucket.download_file(Key="results_5.zip", Filename="results_5.zip")
    with ZipFile("results_5.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"]


def test_single_quotes_in_script(test_bucket, test_mongodb, config_client):
    """
    Search for a specific file using single quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "6",
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "script_path": "user_script.py",
                "config_client": config_client.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]")

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/testbucket/results_6.zip"
    ), "Zipped archive was not created."

    test_bucket.download_file(Key="results_6.zip", Filename="results_6.zip")
    with ZipFile("results_6.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"]
