import pytest

#
# Testing update function in api.py library
#

import os

import requests
import responses

from dtaas.tuilib.api import update


@pytest.fixture(scope="function", autouse=True)
def mocked_response():

    with open("test-update.json", "w") as f:
        f.write("{'content': 'test'}")

    with responses.RequestsMock() as rsps:
        yield rsps
    try:
        os.remove("test-update.json")
    except FileNotFoundError:
        pass


def test_update(mocked_response):
    """
    Update a file
    """

    mocked_response.patch(
        "http://test.com:8080/v1/update",
        status=201,
        body="Metadata Updated Succesfully",
    )

    response = update(
        ip="test.com",
        token="not-necessary",
        file="test-update.txt",
        json_data="test-update.json",
    )

    assert response.status_code == 201
    assert response.text == "Metadata Updated Succesfully"


def test_update_nonexisting(mocked_response):
    """
    Update a file which does not exist on the data lake
    """

    mocked_response.patch(
        "http://test.com:8080/v1/update",
        status=400,
        body="Update failed, file not found. Please use POST method to create a new entry",
    )

    response = update(
        ip="test.com",
        token="not-necessary",
        file="test-update.txt",
        json_data="test-update.json",
    )

    assert response.status_code == 400
    assert response.text == "Update failed, file not found. Please use POST method to create a new entry"


def test_update_without_metadata(mocked_response):
    """
    Update a file without its metadata
    """

    with pytest.raises(TypeError):
        update(
            ip="test.com",
            token="not-necessary",
            file="test-update.txt",
        )


def test_update_without_file(mocked_response):
    """
    Update a file without the filename
    """

    with pytest.raises(TypeError):
        update(
            ip="test.com",
            token="not-necessary",
            json_data="test-update.json",
        )


def test_update_nonexistent(mocked_response):
    """
    Update using a non-existent file
    """

    with pytest.raises(FileNotFoundError):
        update(
            ip="test.com",
            token="not-necessary",
            file="test-update.txt",
            json_data="test-nonexistent.json",
        )


def test_wrong_ip(mocked_response):
    """
    Update using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        update(
            ip="wrong.com",
            token="not-necessary",
            file="test-update.txt",
            json_data="test-update.json",
        )
