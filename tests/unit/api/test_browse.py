import pytest

#
# Testing browse function in api.py library
#

import requests
import responses
from responses import matchers

from dlaas.tuilib.api import browse


@pytest.fixture(scope="function", autouse=True)
def mocked_response():
    with responses.RequestsMock() as rsps:
        yield rsps


def test_browse_all(mocked_response):
    """
    Browse all files
    """

    mocked_response.get(
        "http://test.com:8080/v1/browse_files",
        body="Filter: None\nFiles:  - test_browse_1.txt\n  - test_browse_2.txt\n",
        status=200,
    )

    response = browse(
        ip="test.com",
        token="not-necessary",
    )

    assert response.status_code == 200
    assert response.text == "Filter: None\nFiles:  - test_browse_1.txt\n  - test_browse_2.txt\n"


def test_browse_single(mocked_response):
    """
    Browse single file using filters
    """

    mocked_response.get(
        "http://test.com:8080/v1/browse_files",
        body="Filter: None\nFiles:  - test_browse_1.txt\n",
        status=200,
        match=[matchers.query_param_matcher({"filter": "category = dog"})],
    )

    response = browse(
        ip="test.com",
        token="not-necessary",
        filter="category = dog",
    )

    assert response.status_code == 200
    assert response.text == "Filter: None\nFiles:  - test_browse_1.txt\n"


def test_wrong_ip(mocked_response):
    """
    Browse using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        browse(
            ip="wrong.com",
            token="not-necessary",
        )
