import pytest

#
# Testing replace function in api.py library
#

import os

import requests
import responses

from dtaas.tuilib.api import replace


@pytest.fixture(scope="function", autouse=True)
def mocked_response():

    with open("test-replace.txt", "w") as f:
        f.write("test content")
    with open("test-replace.json", "w") as f:
        f.write("{'content': 'test'}")

    with responses.RequestsMock() as rsps:
        yield rsps
    try:
        os.remove("test-replace.txt")
        os.remove("test-replace.json")
    except FileNotFoundError:
        pass


def test_replace(mocked_response):
    """
    Replace a file
    """

    mocked_response.put(
        "http://test.com:8080/v1/replace",
        status=201,
        body="File and Metadata replacement successful",
    )

    response = replace(
        ip="test.com",
        token="not-necessary",
        file="test-replace.txt",
        json_data="test-replace.json",
    )

    assert response.status_code == 201
    assert response.text == "File and Metadata replacement successful"


def test_replace_nonexisting(mocked_response):
    """
    Replace a file which does not exist on the data lake
    """

    mocked_response.put(
        "http://test.com:8080/v1/replace",
        status=400,
        body="Replacement failed, file not found. Please use POST method to create a new entry",
    )

    response = replace(
        ip="test.com",
        token="not-necessary",
        file="test-replace.txt",
        json_data="test-replace.json",
    )

    assert response.status_code == 400
    assert response.text == "Replacement failed, file not found. Please use POST method to create a new entry"


def test_replace_without_metadata(mocked_response):
    """
    Replace a file without its metadata
    """

    with pytest.raises(TypeError):
        replace(
            ip="test.com",
            token="not-necessary",
            file="test-replace.txt",
        )


def test_replace_without_file(mocked_response):
    """
    Replace a file without the filename
    """

    with pytest.raises(TypeError):
        replace(
            ip="test.com",
            token="not-necessary",
            json_data="test-replace.json",
        )


def test_replace_nonexistent(mocked_response):
    """
    Replace using a non-existent file
    """

    with pytest.raises(FileNotFoundError):
        replace(
            ip="test.com",
            token="not-necessary",
            file="test-replace.txt",
            json_data="test-nonexistent.json",
        )


def test_wrong_ip(mocked_response):
    """
    Replace using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        replace(
            ip="wrong.com",
            token="not-necessary",
            file="test-replace.txt",
            json_data="test-replace.json",
        )
