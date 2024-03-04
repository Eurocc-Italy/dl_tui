import pytest

#
# Testing query function in api.py library
#

import os

import requests
import responses

from dtaas.tuilib.api import query


@pytest.fixture(scope="function", autouse=True)
def mocked_response():

    with open("test-query.txt", "w") as f:
        f.write("SELECT * FROM metadata")
    with open("test-query.py", "w") as f:
        f.write("def main(files_in):\n  return files_in")

    with responses.RequestsMock() as rsps:
        yield rsps
    try:
        os.remove("test-query.txt")
        os.remove("test-query.py")
    except FileNotFoundError:
        pass


def test_query(mocked_response):
    """
    Send query only
    """

    mocked_response.post(
        "http://test.com:8080/v1/query_and_process",
        status=200,
        body="Files processed successfully, ID: 12345",
    )

    response = query(
        ip="test.com",
        token="not-necessary",
        query_file="test-query.txt",
    )

    assert response.status_code == 200
    assert response.text == "Files processed successfully, ID: 12345"


def test_query_and_process(mocked_response):
    """
    Send query + Python script
    """

    mocked_response.post(
        "http://test.com:8080/v1/query_and_process",
        status=200,
        body="Files processed successfully, ID: 12345",
    )

    response = query(
        ip="test.com",
        token="not-necessary",
        query_file="test-query.txt",
        python_file="test-query.py",
    )

    assert response.status_code == 200
    assert response.text == "Files processed successfully, ID: 12345"


def test_query_without_file(mocked_response):
    """
    Send query with Python script only
    """

    with pytest.raises(TypeError):
        query(
            ip="test.com",
            token="not-necessary",
            python_file="test-query.py",
        )


def test_query_nonexistent(mocked_response):
    """
    Replace using a non-existent file
    """

    with pytest.raises(FileNotFoundError):
        query(
            ip="test.com",
            token="not-necessary",
            query_file="test-query.txt",
            python_file="test-nonexistent.py",
        )


def test_wrong_ip(mocked_response):
    """
    Replace using the wrong IP
    """

    with pytest.raises(requests.exceptions.ConnectionError):
        query(
            ip="wrong.com",
            token="not-necessary",
            query_file="test-query.txt",
            python_file="test-query.py",
        )