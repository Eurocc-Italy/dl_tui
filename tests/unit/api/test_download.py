import pytest

#
# Testing download function in api.py library
#

import os

import requests
import responses
from responses import matchers

from dlaas.tuilib.api import download


@pytest.fixture(scope="function", autouse=True)
def mocked_response():
    with responses.RequestsMock() as rsps:
        yield rsps
    try:
        os.remove("test-download.txt")
    except FileNotFoundError:
        pass


def test_download(mocked_response):
    """
    Download a file
    """

    mocked_response.get(
        "http://test.com:8080/v1/download",
        body="test content",
        status=200,
        match=[matchers.query_param_matcher({"file_name": "test-download.txt"})],
    )

    response = download(
        ip="test.com",
        token="not-necessary",
        file="test-download.txt",
    )

    assert response.status_code == 200
    assert response.text == "test content"
    with open("test-download.txt") as f:
        assert f.read() == "test content"


def test_download_not_existing(mocked_response):
    """
    Download a file which does not exist
    """

    mocked_response.get(
        "http://test.com:8080/v1/download",
        body="File path not found in the database\n",
        status=404,
        match=[matchers.query_param_matcher({"file_name": "test-nonexistent.txt"})],
    )

    response = download(
        ip="test.com",
        token="not-necessary",
        file="test-nonexistent.txt",
    )

    assert response.status_code == 404
    assert response.text == "File path not found in the database\n"


def test_wrong_ip(mocked_response):
    """
    Download using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        download(
            ip="wrong.com",
            token="not-necessary",
            file="test-nonexistent.txt",
        )
