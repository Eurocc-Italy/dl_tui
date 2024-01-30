import pytest

#
# Testing the upload_results function in module server.py
#
# NOTE: remember to delete the entries from the S3 bucket after testing!
# You can use a script such as:
#
#   import boto3
#   s3 = boto3.client("s3", endpoint_url="{s3_endpoint_url}")
#   keys = [entry["Key"] for entry in s3.list_objects_v2(Bucket="{s3_bucket}")["Contents"]]
#   keys_to_delete = [key for key in keys if key.startswith("results_DTAAS-TUI-TEST-")]
#   for key in keys_to_delete:
#       s3.delete_object(Bucket="{s3_bucket}", Key=key)
#

import os
import json
from dtaas.tuilib.server import create_remote_directory, copy_json_input, copy_user_script, launch_job, upload_results
from pymongo import MongoClient
from dtaas.tuilib.common import Config


@pytest.fixture(scope="function")
def real_mongodb():
    """Setting up the MongoDB client pointing to the database where actual data should be stored

    Yields
    ------
    Collection
        MongoDB Collection on which to run the tests
    """
    config = Config("client")
    client = MongoClient(f"mongodb://{config.user}:{config.passwd}@{config.ip}:{config.port}/")
    collection = client[config.database][config.collection]

    # run tests
    yield collection

    collection.delete_many({"job_id": {"$regex": "DTAAS-TUI-TEST-*"}})


def test_just_search(config_server, config):
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-upload_results",
                "sql_query": "SELECT * FROM metadata WHERE id = 1 OR id = 2",
                "config_server": config_server.__dict__,
                "config": config.__dict__,
            },
            f,
        )

    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")
    _, _, slurm_job_id = launch_job(json_path="input.json")

    stdout, stderr = upload_results(json_path="input.json", slurm_job_id=slurm_job_id)

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that RESULTS_UPLOADED file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-just_search/RESULTS_UPLOADED'"
            )
            == 0
        ):
            break

        # checking that entry is present in MongoDB database
        assert (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-just_search/results_DTAAS-TUI-TEST-upload_results.zip'"
            )
            == 0
        ), "Results file not found"
