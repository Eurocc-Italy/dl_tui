import pytest

#
# Testing save_output function in client.py library
#

from dtaas.tuilib.client import save_output
import os
from zipfile import ZipFile


def test_save_output(generate_test_files, empty_bucket):
    """
    Test that the function actually generates a zip file.
    """

    save_output(
        files_out=generate_test_files,
        s3_endpoint_url="https://s3.amazonaws.com",
        s3_bucket="test_bucket",
        job_id=1,
    )

    assert len(empty_bucket.list_objects_v2(Bucket="test_bucket")["Contents"]) == 1, "Zipped archive was not uploaded."

    assert (
        empty_bucket.list_objects_v2(Bucket="test_bucket")["Contents"][0]["Key"] == "results_1.zip"
    ), "Zipped archive was not uploaded."

    empty_bucket.download_file(Bucket="test_bucket", Key="results_1.zip", Filename="results_1.zip")

    with ZipFile("results_1.zip", "r") as archive:
        filelist = archive.namelist()
        filelist.sort()  # For some reason, the zip contains the files in reverse order...
        assert filelist == ["TESTFILE_1.txt", "TESTFILE_2.txt"]

    os.remove("results.zip")
    os.remove("results_1.zip")


def test_nonexistent_files(empty_bucket):
    """
    Test the function with nonexistent files (e.g., from incorrect return in user_script `main`).
    UPDATED: code no longer raises the exception,
    """

    save_output(
        files_out=["test1", "test2"],
        s3_endpoint_url="https://s3.amazonaws.com",
        s3_bucket="test_bucket",
        job_id=2,
    )

    assert len(empty_bucket.list_objects_v2(Bucket="test_bucket")["Contents"]) == 1, "Zipped archive was not uploaded."

    assert (
        empty_bucket.list_objects_v2(Bucket="test_bucket")["Contents"][0]["Key"] == "results_2.zip"
    ), "Zipped archive was not uploaded."

    empty_bucket.download_file(Bucket="test_bucket", Key="results_2.zip", Filename="results_2.zip")

    with ZipFile("results_2.zip", "r") as archive:
        filelist = archive.namelist()
        assert filelist == []

    os.remove("results.zip")
    os.remove("results_2.zip")
