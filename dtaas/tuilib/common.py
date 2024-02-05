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


class UserInput:
    """Class containing command-line input arguments passed as JSON-formatted dictionary

    Attributes
    ----------
    id : str
        Unique id of the run (preferably of the UUID.hex type)
    sql_query : str
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
            data (Dict[str, str]): dictionary with the user input (id, sql_query, script, config)
        """
        logger.debug(f"Received input dict: {data}")

        self.id = data["id"]
        self.sql_query = data["sql_query"]

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
        logger.debug(f"UserInput.sql_query: {self.sql_query}")
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
    def from_json(cls, json_path: str):
        """Class constructor from JSON file.
        Expects the path to a JSON file as command line argument

        Parameters
        ----------
        json_path : str
            Path to the JSON file with the input info

        Returns
        -------
        UserInput
            UserInput instance initialized directly from command-line

        Raises
        ------
        TypeError
            if the user does not provide a .json file
        """

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
        """Load default configuration as found in /etc/default.
        If present, overwrites these defaults with the contents of ~/.config/dtaas-tui/config_<version>.json.

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
            base_config: Dict[str, str] = json.load(f)

        # TODO: write tests for this
        if os.path.exists(f"{os.environ['HOME']}/.config/dtaas-tui/config_{version}.json"):
            with open(f"{os.environ['HOME']}/.config/dtaas-tui/config_{version}.json", "r") as f:
                config: Dict[str, str] = json.load(f)

            for key in config:
                if key not in base_config:
                    raise KeyError(f"Unknown parameter in configuration file: '{key}'")

            base_config.update(config)

        return base_config

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
