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
        - Switch to the test_db database:
            use test_db
        - Insert the entries:
            db.test_coll.insertMany(
                [
                    {
                        "id": 1, 
                        "s3_key": "test1.txt", 
                        "path": "<repo-path>/dtaas-tui/tests/utils/sample_files/test1.txt"
                    },
                    {
                        "id": 2, 
                        "s3_key": "test2.txt", 
                        "path": "<repo-path>/dtaas-tui/tests/utils/sample_files/test2.txt"
                    }
                ]
            )
        
    2. An S3 server (we recommend setting one up locally via localstack in a docker container) containing the
    corresponding S3 keys in a test bucket (test1.txt and test2.txt). To generate the files on the spot, follow
    these instructions:
        - Move to the dtaas-tui/tests/utils/localstack folder and start the docker environment: 
            docker-compose up -d
        - Create test bucket: 
            awslocal s3api create-bucket --bucket testbucket
        - Move to the ../sample_files folder and upload test files to bucket: 
            awslocal s3api put-object --bucket testbucket --key test[1|2].txt --body test[1|2].txt
        - Check that files were correctly uploaded:
            awslocal s3api list-objects --bucket testbucket
"""


@pytest.fixture(scope="function", autouse=True)
def create_tmpdir():
    """Creates temporary directory for storing results file, and then deletes it"""
    os.makedirs(f"{os.getcwd()}/tests/integration/testbucket", exist_ok=True)
    yield
    shutil.rmtree(f"{os.getcwd()}/tests/integration/testbucket")


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
            "s3_endpoint_url": "https://s3.amazonaws.com/",
            "s3_bucket": "testbucket",
            "pfs_prefix_path": f"{os.path.dirname(os.path.abspath(__file__))}/../",
        }
    )
    return config_client


def test_search_only(config_client):
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
        f"{os.getcwd()}/tests/integration/testbucket/results_1.zip"
    ), "Zipped archive was not created."

    with ZipFile(f"{os.getcwd()}/tests/integration/testbucket/results_1.zip", "r") as archive:
        assert archive.namelist() == [
            "test1.txt",
            "test2.txt",
        ]


def test_return_first():
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM test_coll WHERE id = 554625 OR id = 222564",
                "script_path": "user_script.py",
                "config_client": {"ip": "localhost"},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_SQL(config_client):
    """
    Search for a specific file using double quotes in SQL query
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": 'SELECT * FROM test_coll WHERE captured = "2013-11-14 16:03:19"',
                "config_client": {"ip": "localhost"},
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_SQL():
    """
    Search for a specific file using single quotes in SQL query.
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM test_coll WHERE captured = '2013-11-14 16:03:19'",
                "config_client": {"ip": "localhost"},
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_double_quotes_in_script():
    """
    Search for a specific file using double quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM test_coll WHERE id = 554625",
                "script_path": "user_script.py",
                "config_client": {"ip": "localhost"},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write('def main(files_in):\n files_out=files_in.copy()\n print("HELLO!")\n return [files_out[0]]')

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")


def test_single_quotes_in_script():
    """
    Search for a specific file using single quotes in user script
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM test_coll WHERE id = 554625",
                "script_path": "user_script.py",
                "config_client": {"ip": "localhost"},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n print('HELLO!')\n return [files_out[0]]")

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
