import pytest

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
                        "path": "<repo-path>/dtaas-tui/tests/sample_files/test1.txt"
                    },
                    {
                        "id": 2, 
                        "s3_key": "test2.txt", 
                        "path": "<repo-path>/dtaas-tui/tests/sample_files/test2.txt"
                    }
                ]
            )
        
    2. An S3 server (we recommend setting one up locally via localstack in a docker container) containing the
    corresponding S3 keys in a test bucket (test1.txt and test2.txt). To generate the files on the spot, follow
    these instructions:
        - Start your localstack environment: 
            localstack start -d (if you want to run in detached mode)
        - Create test bucket: 
            awslocal s3api create-bucket --bucket test-bucket
        - Upload test files to bucket: 
            awslocal s3api put-object --bucket test-bucket --key test[1|2].txt --body test[1|2].txt
        - Check that files were correctly uploaded:
            awslocal s3api list-objects --bucket test-bucket
"""

import os
import json
from zipfile import ZipFile


def test_search_only():
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "1",
                "sql_query": "SELECT * FROM test_coll WHERE id = 1 OR id = 2",
                "config_client": {
                    "ip": "localhost",
                    "pfs_prefix_path": f"{os.getcwd()}/tests/integration/",
                    "s3_bucket": "test-bucket",
                },
            },
            f,
        )

    os.system(f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py input.json")

    assert os.path.exists(
        f"{os.getcwd()}/tests/integration/test-bucket/results_1.zip"
    ), "Zipped archive was not created."

    with ZipFile(f"{os.getcwd()}/tests/integration/test-bucket/results_1.zip", "r") as archive:
        assert archive.namelist() == [
            "test1.txt",
            "test2.txt",
        ]

    os.remove(f"{os.getcwd()}/tests/integration/test-bucket/results_1.zip")


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
