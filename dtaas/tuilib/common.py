"""
Various functions and utilities used throughout the code

Author: @lbabetto
"""

import os
import sys
import json
import pickle
from typing import Dict

import logging

logger = logging.getLogger(__name__)


def sanitize_string(string: str):
    """Sanitize string for use on shell, replacing special characters

    Parameters
    ----------
    string : str
        "raw" string to be sanitized

    Returns
    -------
    str
        sanitized string
    """
    sanitized_string = (
        string.replace("'", r"\'")
        .replace('"', r"\"")
        .replace(r"\n", r"\\n")
        .replace("*", r"\*")
        .replace("(", r"\(")
        .replace(")", r"\)")
    )
    return sanitized_string


class UserInput:
    """Class containing command-line input arguments passed as JSON-formatted dictionary

    Parameters
    ----------
    data : Dict[str, str]
        dictionary with the user input (query, script, ID)

    Attributes
    ----------
    query : str
        SQL query
    script : str
        Python script with the analysis on the files returned by the SQL query
    """

    def __init__(self, data: Dict[str, str]) -> None:
        logger.debug(f"Received input dict: {data}")
        self.id = data["ID"]
        self.query = data["query"]

        try:
            self.script = data["script"]
        except KeyError:
            self.script = None

    @classmethod
    def from_cli(cls):
        """Class constructor from command-line argument.
        Expects a properly JSON-formatted dictionary as input.

        Returns
        -------
        UserInput
            UserInput instance initialized directly from command-line
        """
        user_input = " ".join(sys.argv[1:])
        logger.info(f"Received input from CLI: {user_input}")
        data = json.loads(user_input)
        return cls(data)

    # TODO: this is not done yet
    @classmethod
    def from_bytes(cls, data_bytes: bytes):
        """Class constructor from serialized pickle object.

        Parameters
        ----------
        data_bytes : bytes
            _description_

        Returns
        -------
        UserInput
            UserInput instance initialized providing a pickle object
        """
        data_dict = pickle.loads(data_bytes)
        logger.info(f"Raw input: {data_bytes}")
        logger.info(f"Processed input: {data_dict}")
        return cls(data_dict)


class Config:
    """Class containing configuration info for client/server.
    It reads and loads the default etc/default/config_{client/server}.json file and overwrites its data
    with any content present in /etc/config_{client/server}.json

    Parameters
    ----------
    version : str
        specifies whether the config is relative to the client or server version

    Attributes
    ----------
    All contents of the dictionary will be available as Config.key attributes
    """

    def __init__(self, version: str) -> Dict[str, str]:
        if version not in ["client", "server"]:
            raise NameError("config file only available for 'client' or 'server'")

        data = self.load_config(version)

        for key, value in data.items():
            setattr(self, key, value)

    def load_config(self, version):
        # read default configuration file
        with open(
            f"{os.path.dirname(__file__)}/../etc/default/config_{version}.json", "r"
        ) as f:
            config = json.load(f)

        # read custom configuration file, if present
        if os.path.exists(f"{os.path.dirname(__file__)}/../etc/config_{version}.json"):
            with open(
                f"{os.path.dirname(__file__)}/../etc/config_{version}.json", "r"
            ) as f:
                new_config = json.load(f)
                config.update(new_config)

        return config
