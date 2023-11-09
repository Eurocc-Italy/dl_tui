import pytest

#
# Testing Config class in common.py library
#

import os
import json
from dtaas.tuilib.common import Config


@pytest.fixture(scope="function")
def default_client():
    config = {
        "user": "user",
        "password": "passwd",
        "ip": "131.175.207.101",
        "port": "27017",
        "database": "datalake",
        "collection": "metadata",
    }
    return config


@pytest.fixture(scope="function")
def default_server():
    config = {
        "user": "lbabetto",
        "host": "login02-ext.g100.cineca.it",
        "repo_dir": "~/REPOS/DTaaS_TUI/dtaas",
        "venv_path": "~/virtualenvs/dtaas/bin/activate",
        "ssh_key": "~/.ssh/luca-g100",
        "partition": "g100_usr_prod",
        "account": "cin_staff",
        "mail": "NO",
        "walltime": "01:00:00",
        "nodes": 1,
    }
    return config


@pytest.fixture(scope="function")
def custom_client():
    with open(f"{os.path.dirname(__file__)}/../../../dtaas/etc/config_client.json", "w") as f:
        json.dump({"ip": "localhost"}, f)
    config = {
        "user": "user",
        "password": "passwd",
        "ip": "localhost",
        "port": "27017",
        "database": "datalake",
        "collection": "metadata",
    }
    yield config
    os.remove(f"{os.path.dirname(__file__)}/../../../dtaas/etc/config_client.json")


@pytest.fixture(scope="function")
def custom_server():
    with open(f"{os.path.dirname(__file__)}/../../../dtaas/etc/config_server.json", "w") as f:
        json.dump({"account": "EUCC_staff"}, f)
    config = {
        "user": "lbabetto",
        "host": "login02-ext.g100.cineca.it",
        "repo_dir": "~/REPOS/DTaaS_TUI/dtaas",
        "venv_path": "~/virtualenvs/dtaas/bin/activate",
        "ssh_key": "~/.ssh/luca-g100",
        "partition": "g100_usr_prod",
        "account": "EUCC_staff",
        "mail": "NO",
        "walltime": "01:00:00",
        "nodes": 1,
    }
    yield config
    os.remove(f"{os.path.dirname(__file__)}/../../../dtaas/etc/config_server.json")


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

    for key, val in config_true.items():
        assert getattr(config_test, key) == val


def test_custom_server(custom_server):
    """
    Test that the custom config file for the server is correctly loaded, if present.
    """

    config_true = custom_server
    config_test = Config(version="server")

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
