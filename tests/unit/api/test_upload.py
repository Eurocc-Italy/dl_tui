import pytest

#
# Testing upload function in api.py library
#

import os

import requests
import responses

from dtaas.tuilib.api import upload


@pytest.fixture(scope="function", autouse=True)
def mocked_response():

    with open("test-upload.txt", "w") as f:
        f.write("test content")
    with open("test-upload.json", "w") as f:
        f.write("{'content': 'test'}")

    with responses.RequestsMock() as rsps:
        yield rsps
    try:
        os.remove("test-upload.txt")
        os.remove("test-upload.json")
    except FileNotFoundError:
        pass


def test_upload(mocked_response):
    """
    Upload a file
    """

    mocked_response.post(
        "http://test.com:8080/v1/upload",
        status=201,
        body="File and Metadata upload successful",
    )

    response = upload(
        ip="test.com",
        token="not-necessary",
        file="test-upload.txt",
        json_data="test-upload.json",
    )

    assert response.status_code == 201
    assert response.text == "File and Metadata upload successful"


def test_upload_existing(mocked_response):
    """
    Upload a file which already exists on the data lake
    """

    mocked_response.post(
        "http://test.com:8080/v1/upload",
        status=400,
        body="Upload Failed, entry is already present. Please use PUT method to update an existing entry",
    )

    response = upload(
        ip="test.com",
        token="not-necessary",
        file="test-upload.txt",
        json_data="test-upload.json",
    )

    assert response.status_code == 400
    assert response.text == "Upload Failed, entry is already present. Please use PUT method to update an existing entry"


def test_upload_without_metadata(mocked_response):
    """
    Upload a file without its metadata
    """

    with pytest.raises(TypeError):
        upload(
            ip="test.com",
            token="not-necessary",
            file="test-upload.txt",
        )


def test_upload_without_file(mocked_response):
    """
    Upload a file without the file name
    """

    with pytest.raises(TypeError):
        upload(
            ip="test.com",
            token="not-necessary",
            json_data="test-upload.json",
        )


def test_upload_nonexistent(mocked_response):
    """
    Upload a non-existent file
    """

    with open("test-upload.json", "w") as f:
        f.write("{'content': 'test'}")

    with pytest.raises(FileNotFoundError):
        upload(
            ip="test.com",
            token="not-necessary",
            file="test-nonexistent.txt",
            json_data="test-upload.json",
        )


def test_wrong_ip(mocked_response):
    """
    Upload using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        upload(
            ip="wrong.com",
            token="not-necessary",
            file="test-upload.txt",
            json_data="test-upload.json",
        )
