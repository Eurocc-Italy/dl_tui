import pytest

#
# Testing Config class in common.py library
#

import os
import json
from dtaas.tuilib.common import Config


@pytest.fixture(scope="function")
def default_client():
    with open(f"{os.path.dirname(__file__)}/../../../dtaas/etc/default/config_client.json", "r") as f:
        config = json.load(f)
    return config


@pytest.fixture(scope="function")
def default_server():
    with open(f"{os.path.dirname(__file__)}/../../../dtaas/etc/default/config_server.json", "r") as f:
        config = json.load(f)
    return config


@pytest.fixture(scope="function")
def custom_client():
    with open(f"{os.path.dirname(__file__)}/../../../dtaas/etc/default/config_client.json", "r") as f:
        config = json.load(f)
        config.update({"ip": "localhost"})
    return config


@pytest.fixture(scope="function")
def custom_server():
    with open(f"{os.path.dirname(__file__)}/../../../dtaas/etc/default/config_server.json", "r") as f:
        config = json.load(f)
        config.update({"account": "EUCC_staff"})
    return config


def test_default_client(default_client):
    """
    Test that the default config file for the client is correctly loaded.
    """

    config_true = default_client
    config_test = Config(version="client")

    for key, val in config_true.items():
        assert getattr(config_test, key) == val


def test_default_server(default_server):
    """
    Test that the default config file for the server is correctly loaded.
    """

    config_true = default_server
    config_test = Config(version="server")

    for key, val in config_true.items():
        assert getattr(config_test, key) == val


def test_custom_client(custom_client):
    """
    Test that the custom config file for the client is correctly loaded, if present.
    """

    config_true = custom_client
    config_test = Config(version="client")
    config_test.load_custom_config({"ip": "localhost"})

    for key, val in config_true.items():
        assert getattr(config_test, key) == val


def test_custom_server(custom_server):
    """
    Test that the custom config file for the server is correctly loaded, if present.
    """

    config_true = custom_server
    config_test = Config(version="server")
    config_test.load_custom_config({"account": "EUCC_staff"})

    for key, val in config_true.items():
        assert getattr(config_test, key) == val


def test_manual_custom(default_client):
    """
    Test that manually changing values works.
    """

    config_true = default_client
    default_client["ip"] = "localhost"

    config_test = Config(version="client")
    config_test.ip = "localhost"

    for key, val in config_true.items():
        assert getattr(config_test, key) == val


def test_wrong_version():
    """
    Test that entering a wrong name version raises an exception.
    """
    with pytest.raises(NameError):
        Config(version="test")


def test_wrong_key(custom_client):
    """
    Test that the program crashes if an unknown configuration option is provided.
    """
    with pytest.raises(KeyError):
        config_test = Config(version="client")
        config_test.load_custom_config({"account": "EUCC_staff"})
