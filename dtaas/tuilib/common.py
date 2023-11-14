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
    For server, escape character \ must itself be escaped, as the string
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

    Parameters
    ----------
    data : Dict[str, str]
        dictionary with the user input (query, script, ID)

    Attributes
    ----------
    ID : str
        Unique ID of the run (preferably of the UUID.hex type)
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

        self.version = version

        data = self.load_config(version)

        for key, value in data.items():
            setattr(self, key, value)

    def __str__(self):
        info = " --- DTaaS TUI configuration ---\n"
        for key, val in self.__dict__.items():
            info += f"{key:16}: {val}\n"
        return info

    def load_config(self, version):
        # read default configuration file
        with open(f"{os.path.dirname(__file__)}/../etc/default/config_{version}.json", "r") as f:
            config = json.load(f)

        # read custom configuration file, if present
        if os.path.exists(f"{os.path.dirname(__file__)}/../etc/config_{version}.json"):
            with open(f"{os.path.dirname(__file__)}/../etc/config_{version}.json", "r") as f:
                new_config = json.load(f)
                config.update(new_config)

        return config
