"""
Various functions and utilities used throughout the code

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import os
import sys
import json
from typing import Dict


def sanitize_string(version: str, string: str):
    """Sanitize string for use on shell, replacing special characters.
    For server, escape character \\ must itself be escaped, as the string
    passes via two shells, one in the ssh command call and one within the
    slurm script.

    Parameters
    ----------
    version : str
        specifies whether the string must be "cleaned up" for client or server
    string : str
        "raw" string to be sanitized

    Returns
    -------
    str
        sanitized string
    """
    if version not in ["client", "server"]:
        raise NameError("version must either be 'client' or 'server'")

    sanitized_string = string.replace("'", r"\'" if version == "client" else r"\\\'")
    sanitized_string = sanitized_string.replace('"', r"\"" if version == "client" else r"\\\"")
    sanitized_string = sanitized_string.replace(r"\n", r"\\n" if version == "client" else r"\\\\n")
    sanitized_string = sanitized_string.replace("*", r"\*")
    sanitized_string = sanitized_string.replace("(", r"\(")
    sanitized_string = sanitized_string.replace(")", r"\)")

    return sanitized_string


class UserInput:
    """Class containing command-line input arguments passed as JSON-formatted dictionary

    Attributes
    ----------
    ID : str
        Unique ID of the run (preferably of the UUID.hex type)
    query : str
        SQL query
    script_path : str
        Path to the Python script with the analysis on the files returned by the SQL query
    config_client : dict
        dictionary with custom configuration options for client version
    config_server : dict
        dictionary with custom configuration options for server version
    """

    def __init__(self, data: Dict[str, str]) -> None:
        """_summary_

        Args:
            data (Dict[str, str]): dictionary with the user input (ID, query, script, config)
        """
        logger.debug(f"Received input dict: {data}")

        self.id = data["ID"]
        self.query = data["query"]

        try:
            self.script_path = data["script_path"]
        except KeyError:  # no script provided
            self.script_path = None

        try:
            self.config_client = json.loads(data["config_client"].replace("'", '"'))
        except KeyError:  # no custom config provided
            self.config_client = None
        except AttributeError:  # config was initialized manually
            self.config_client = data["config_client"]

        try:
            self.config_server = json.loads(data["config_server"].replace("'", '"'))
        except KeyError:  # no custom config provided
            self.config_server = None
        except AttributeError:  # config was initialized manually
            self.config_server = data["config_server"]

        logger.debug(f"UserInput.id: {self.id}")
        logger.debug(f"UserInput.query: {self.query}")
        logger.debug(f"UserInput.script_path: {self.script_path}")
        logger.debug(f"UserInput.config_client: {self.config_client}")
        logger.debug(f"UserInput.config_server: {self.config_server}")

    @classmethod
    def from_cli(cls):
        """Class constructor from command-line argument.
        Expects a properly JSON-formatted dictionary as command line argument.

        Returns
        -------
        UserInput
            UserInput instance initialized directly from command-line
        """
        user_input = " ".join(sys.argv[1:])
        logger.info(f"Received input from CLI: {user_input}")
        data = json.loads(user_input)
        return cls(data)

    @classmethod
    def from_json(cls):
        """Class constructor from JSON file.
        Expects the path to a JSON file as command line argument

        Returns
        -------
        UserInput
            UserInput instance initialized directly from command-line

        Raises
        ------
        TypeError
            if the user does not provide a .json file
        """

        json_path = sys.argv[1]

        if not json_path.endswith(".json"):
            raise TypeError("Provided input is not a .json file")

        with open(json_path, "r") as f:
            data = json.load(f)
        logger.info(f"Received input from JSON file: {data}")

        return cls(data)


class Config:
    """Class containing configuration info for client/server.
    At initialization, it reads and loads the default etc/default/config_{client/server}.json file.
    Settings can be overwritten by passing a dictionary to the load_custom_config method
    """

    def __init__(self, version: str) -> None:
        """Initialization for Config class

        Parameters
        ----------
        version : str
            specifies whether the config is relative to the client or server version

        Raises
        ------
        NameError
            If version is not 'client' or 'server' raises an error
        """
        if version not in ["client", "server"]:
            raise NameError("config file only available for 'client' or 'server'")

        self.version = version

        default = self.load_default_config(version=version)
        for key, value in default.items():
            setattr(self, key, value)

    def __str__(self):
        return str(self.__dict__)

    def load_default_config(self, version: str) -> Dict[str, str]:
        """Load default configuration as found in /etc/default

        Parameters
        ----------
        version : str
            specify whether the configuration is for client/server version

        Returns
        -------
        Dict[str, str]
            dictionary with the configuration info
        """
        with open(f"{os.path.dirname(__file__)}/../etc/default/config_{version}.json", "r") as f:
            config = json.load(f)
        return config

    def load_custom_config(self, custom_config: Dict[str, str]):
        """Overwrites the default configurations with custom options

        Parameters
        ----------
        custom_config : Dict[str, str]
            dictionary containing the settings to overwrite
        """
        for key in custom_config:
            if key not in self.__dict__:
                raise KeyError(f"Unknown parameter in custom configuration: '{key}'")
        self.__dict__.update(custom_config)
