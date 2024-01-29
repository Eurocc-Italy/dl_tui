import pytest

#
# Testing wrapper function in wrapper.py
#

from dtaas.tuilib.client import wrapper
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


def test_search_specific_files(test_mongodb):
    """
    Search for two specific files
    """
    query = "SELECT * FROM metadata WHERE id = 1 OR id = 2"

    wrapper(
        collection=test_mongodb,
        sql_query=query,
        pfs_prefix_path=f"{ROOT_DIR}/",
        s3_bucket="testbucket",
        job_id=1,
    )

    assert os.path.exists(f"results_1.zip"), "Zipped archive was not created."

    with ZipFile("results_1.zip", "r") as archive:
        assert archive.namelist() == [
            "test1.txt",
            "test2.txt",
        ]

    os.remove("results_1.zip")


def test_search_specific_files_return_only_first(test_mongodb):
    """
    Search for two specific files and return just the first item
    """
    query = "SELECT * FROM metadata WHERE id = 1 OR id = 2"
    script = "def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]"

    wrapper(
        collection=test_mongodb,
        sql_query=query,
        pfs_prefix_path=f"{ROOT_DIR}/",
        s3_bucket="testbucket",
        job_id=2,
        script=script,
    )

    assert os.path.exists("results_2.zip"), "Zipped archive was not created."

    with ZipFile("results_2.zip", "r") as archive:
        assert archive.namelist() == ["test1.txt"]

    os.remove("results_2.zip")
