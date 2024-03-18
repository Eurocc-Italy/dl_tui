import pytest

#
# Testing wrapper function in wrapper.py
#

from dlaas.tuilib.hpc import wrapper
import os
import shutil
from zipfile import ZipFile

from conftest import ROOT_DIR


@pytest.fixture(scope="function", autouse=True)
def create_tmpdir():
    """Creates temporary directory for storing results file, and then deletes it"""
    os.makedirs(f"{ROOT_DIR}/testbucket", exist_ok=True)
    yield
    shutil.rmtree(f"{ROOT_DIR}/testbucket")


def test_search_specific_files(mock_mongodb):
    """
    Search for two specific files
    """
    query = "SELECT * FROM metadata WHERE id = '1' OR id = '2'"

    wrapper(
        collection=mock_mongodb,
        sql_query=query,
        pfs_prefix_path=ROOT_DIR,
        s3_endpoint_url="https://testurl.com/",
        s3_bucket="test",
        job_id=1,
    )

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

    assert len([_ for _ in mock_mongodb.find({"job_id": 1})]) == 1
    assert mock_mongodb.find_one({"job_id": 1})["path"] == f"{ROOT_DIR}/results_1.zip"
    assert mock_mongodb.find_one({"job_id": 1})["s3_key"] == "results_1.zip"


def test_search_specific_files_return_only_first(mock_mongodb):
    """
    Search for two specific files and return just the first item
    """
    query = "SELECT * FROM metadata WHERE id = '1' OR id = '2'"
    script = "def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"

    wrapper(
        collection=mock_mongodb,
        sql_query=query,
        pfs_prefix_path=ROOT_DIR,
        s3_endpoint_url="https://testurl.com/",
        s3_bucket="test",
        job_id=2,
        script=script,
    )

    assert os.path.exists("results_2.zip"), "Zipped archive was not created."

    with ZipFile("results_2.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"], "Results archive does not contain the expected files."

    os.remove("results_2.zip"), "Results archive does not contain the expected files."

    assert os.path.exists("upload_results_2.py"), "Upload script was not created."

    with open("upload_results_2.py", "r") as f:
        expected = "import boto3\n"
        expected += 's3 = boto3.client(service_name="s3", endpoint_url="https://testurl.com/")\n'
        expected += 's3.upload_file(Filename="results_2.zip", Bucket="test", Key="results_2.zip")'

        actual = f.read()
        assert actual == expected

    assert len([_ for _ in mock_mongodb.find({"job_id": 2})]) == 1
    assert mock_mongodb.find_one({"job_id": 2})["path"] == f"{ROOT_DIR}/results_2.zip"
    assert mock_mongodb.find_one({"job_id": 2})["s3_key"] == "results_2.zip"
