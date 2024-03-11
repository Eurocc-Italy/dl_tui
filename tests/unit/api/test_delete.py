import pytest

#
# Testing delete function in api.py library
#

import requests
import responses
from responses import matchers

from dlaas.tuilib.api import delete


@pytest.fixture(scope="function", autouse=True)
def mocked_response():
    with responses.RequestsMock() as rsps:
        yield rsps


def test_delete(mocked_response):
    """
    Delete a file
    """

    mocked_response.delete(
        "http://test.com:8080/v1/delete",
        body="File and its database entry deleted successfully",
        status=200,
        match=[matchers.query_param_matcher({"file_name": "test-delete.txt"})],
    )

    response = delete(
        ip="test.com",
        token="not-necessary",
        file="test-delete.txt",
    )

    assert response.status_code == 200
    assert response.text == "File and its database entry deleted successfully"


def test_delete_not_existing(mocked_response):
    """
    Delete a file which does not exist
    """

    mocked_response.delete(
        "http://test.com:8080/v1/delete",
        body="File not found\n",
        status=404,
        match=[matchers.query_param_matcher({"file_name": "test-nonexistent.txt"})],
    )

    response = delete(
        ip="test.com",
        token="not-necessary",
        file="test-nonexistent.txt",
    )

    assert response.status_code == 404
    assert response.text == "File not found\n"


def test_wrong_ip(mocked_response):
    """
    Delete using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        delete(
            ip="wrong.com",
            token="not-necessary",
            file="test-nonexistent.txt",
        )
