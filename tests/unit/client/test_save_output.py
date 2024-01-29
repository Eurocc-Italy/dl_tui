import pytest

#
# Testing save_output function in client.py library
#
# TODO: missing S3 sync part

from dtaas.tuilib.client import save_output
import os
import shutil
from zipfile import ZipFile


@pytest.fixture(scope="function", autouse=True)
def create_tmpdir():
    """Creates temporary directory for storing results file, and then deletes it"""
    os.makedirs(f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket", exist_ok=True)
    yield
    shutil.rmtree(f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket")


def test_save_output(empty_bucket):
    """
    Test that the function actually generates a zip file.
    """

    save_output(
        files_out=[
            f"{os.path.dirname(os.path.abspath(__file__))}/../../utils/sample_files/test1.txt",
            f"{os.path.dirname(os.path.abspath(__file__))}/../../utils/sample_files/test2.txt",
        ],
        pfs_prefix_path=f"{os.path.dirname(os.path.abspath(__file__))}/",
        s3_bucket="emptybucket",
        job_id=1,
    )

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket/results_1.zip"
    ), "Zipped archive was not created."

    with ZipFile(f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket/results_1.zip", "r") as archive:
        assert archive.namelist() == [
            "test1.txt",
            "test2.txt",
        ]

    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket/results_1.zip")

    s3_keys = [obj.key for obj in empty_bucket.objects.all()]

    assert len(s3_keys) == 1, "Zipped archive was not uploaded."

    assert s3_keys[0] == "results_1.zip", "Zipped archive was not uploaded."

    empty_bucket.download_file(Key="results_1.zip", Filename="results_1.zip")

    with ZipFile("results_1.zip", "r") as archive:
        filelist = archive.namelist()
        filelist.sort()  # For some reason, the zip contains the files in reverse order...
        assert filelist == ["test1.txt", "test2.txt"]


def test_nonexistent_files(empty_bucket):
    """
    Test the function with nonexistent files (e.g., from incorrect return in user_script `main`).
    UPDATED: code no longer raises the exception,
    """

    save_output(
        files_out=[
            f"{os.path.dirname(os.path.abspath(__file__))}/../../utils/sample_files/notafile.txt",
        ],
        pfs_prefix_path=f"{os.path.dirname(os.path.abspath(__file__))}/",
        s3_bucket="emptybucket",
        job_id=2,
    )

    assert os.path.exists(
        f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket/results_2.zip"
    ), "Zipped archive was not created."

    with ZipFile(f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket/results_2.zip", "r") as archive:
        assert archive.namelist() == []

    os.remove(f"{os.path.dirname(os.path.abspath(__file__))}/emptybucket/results_2.zip")

    s3_keys = [obj.key for obj in empty_bucket.objects.all()]

    assert len(s3_keys) == 1, "Zipped archive was not uploaded."

    assert s3_keys[0] == "results_2.zip", "Zipped archive was not uploaded."

    empty_bucket.download_file(Key="results_2.zip", Filename="results_2.zip")

    with ZipFile("results_2.zip", "r") as archive:
        filelist = archive.namelist()
        assert filelist == []
