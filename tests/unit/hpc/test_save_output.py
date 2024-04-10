import pytest

#
# Testing save_output function in hpc.py library
#

from dlaas.tuilib.hpc import save_output
import os

from zipfile import ZipFile

from conftest import ROOT_DIR


def test_save_output(mock_mongodb):
    """
    Test that the function actually generates a zip file.
    """

    save_output(
        sql_query="QUERY",
        script="SCRIPT",
        files_out=[
            f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
            f"{ROOT_DIR}/tests/utils/sample_files/test2.txt",
        ],
        pfs_prefix_path=ROOT_DIR,
        s3_endpoint_url="https://testurl.com/",
        s3_bucket="test",
        job_id=1,
        collection=mock_mongodb,
    )

    assert os.path.exists("results_1.zip"), "Zipped archive was not created."

    with ZipFile(f"results_1.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            "query_1.txt",
            "test1.txt",
            "test2.txt",
            "user_script_1.py",
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


def test_nonexistent_files(mock_mongodb):
    """
    Test the function with nonexistent files (e.g., from incorrect return in user_script `main`).
    """

    save_output(
        sql_query="QUERY",
        script=None,
        files_out=[
            f"{ROOT_DIR}/tests/utils/sample_files/notafile.txt",
        ],
        pfs_prefix_path=ROOT_DIR,
        s3_endpoint_url="https://testurl.com/",
        s3_bucket="test",
        job_id=2,
        collection=mock_mongodb,
    )

    assert os.path.exists("results_2.zip"), "Zipped archive was not created."

    with ZipFile(f"results_2.zip", "r") as archive:
        assert archive.namelist() == ["query_2.txt"], "Results archive contains unexpected items."

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
