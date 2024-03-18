import pytest

#
# Testing browse function in api.py library
#

import subprocess
import os
import json
from conftest import ROOT_DIR


@pytest.fixture(scope="module", autouse=True)
def setup_testfiles():

    # creating test files
    with open("test.txt", "w") as f:
        f.write("test")
    with open("test2.txt", "w") as f:
        f.write("test2")

    # creating metadata for test files
    with open("test.json", "w") as f:
        json.dump({"category": "dog"}, f)
    with open("test2.json", "w") as f:
        json.dump({"category": "cat"}, f)

    # run tests
    yield

    # delete local test files
    os.remove("test.txt")
    os.remove("test.json")
    os.remove("test2.txt")
    os.remove("test2.json")


def test_upload():
    """
    Upload a file
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py upload file=test.txt json_data=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully uploaded file test.txt.\n"
    assert stderr == b""


def test_upload_again():
    """
    Upload same file again
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py upload file=test.txt json_data=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"Upload Failed, entry is already present. Please use PUT method to update an existing entry"\n\n'
    assert b"400 Client Error: BAD REQUEST for url: http://131.175.205.87:8080/v1/upload" in stderr


def test_download():
    """
    Download a file
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py download file=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully downloaded file test.txt.\n"
    assert stderr == b""


def test_download_nonexistent():
    """
    Download a file not in the data lake
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py download file=test-nonexistent.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"File not found\n\n"
    assert (
        b"404 Client Error: NOT FOUND for url: http://131.175.205.87:8080/v1/download?file_name=test-nonexistent.txt"
        in stderr
    )


def test_replace():
    """
    Replace a file
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py replace file=test.txt json_data=test.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully replaced file test.txt.\n"
    assert stderr == b""


def test_replace_nonexistent():
    """
    Replace a file not on the datalake
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py replace file=test2.txt json_data=test2.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"Replacement failed, file not found. Please use POST method to create a new entry"\n\n'
    assert b"400 Client Error: BAD REQUEST for url: http://131.175.205.87:8080/v1/replace" in stderr


def test_update():
    """
    Update a file
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py update file=test.txt json_data=test2.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully updated metadata for file test.txt.\n"
    assert stderr == b""


def test_update_nonexistent():
    """
    Update a file not on the datalake
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py update file=test2.txt json_data=test2.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"Update failed, file not found. Please use POST method to create a new entry"\n\n'
    assert b"400 Client Error: BAD REQUEST for url: http://131.175.205.87:8080/v1/update" in stderr


def test_browse_all():
    """
    Browse all files
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py browse",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Filter: None\nFiles:\n  - test.txt\n"
    assert stderr == b""


def test_browse_filter():
    """
    Browse data lake using filters
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py browse filter='category = cat'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Filter: category = cat\nFiles:\n  - test.txt\n"
    assert stderr == b""


def test_delete():
    """
    Delete a file
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py delete file=test.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b"Successfully deleted file test.txt.\n"
    assert stderr == b""


def test_delete_nonexistent():
    """
    Delete a file not in the data lake
    """

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui.py delete file=test2.txt",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b'"File path not found in the database"\n\n'
    assert b"404 Client Error: NOT FOUND for url: http://131.175.205.87:8080/v1/delete?file_name=test2.txt" in stderr


def test_wrong_ip():
    """
    Browse using the wrong IP
    """

    with pytest.raises(subprocess.TimeoutExpired):
        stdout, stderr = subprocess.Popen(
            f"{ROOT_DIR}/dlaas/bin/dl_tui.py browse ip=wrong.ip.com",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).communicate(timeout=2)
