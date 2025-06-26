"""
Various functions and utilities used throughout the code

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import os
import sys
import json
import re


def sanitize_dictionary(dictionary: dict[str, str]) -> None:
    """Sanitize dictionary input to make sure no OS command injection is possible. It is intended to
    be used on the self.__dict__ dictionary of Config("server") or Config("hpc") instances.
    For each keyword it performs a regex match to ensure the expected format is found, for example the
    value of the keyword "walltime" must be something like DD-HH:MM:SS, or HH:MM:SS, or MM:SS.

    Parameters
    ----------
    dictionary : dict[str, str]
        Dictionary to be checked. Should be the self.__dict__ of Config("server") or Config("hpc")

    Raises
    ------
    SyntaxError
        if any keyword does not match the expected regex format, raise exception and stop code execution.
    """

    keyword_formats = {
        "version": ["server", "hpc"],
        ###############
        #  config_hpc #
        ###############
        "user": [r"[a-zA-Z0-9_]+"],  # any single word (word: character sequence containing alphanumerics or _)
        "password": [r"[a-zA-Z0-9_]+"],  # any single word
        "ip": [
            r"[a-zA-Z0-9_\.-]+[a-zA-Z0-9_-]+"
        ],  # any word sequence (with - and _) optionally delimited by dots, but not ending with one
        "port": [r"[0-9]+"],  # any number
        "database": [r"[a-zA-Z0-9_]+"],  # any single word
        "collection": [r"[a-zA-Z0-9_]+"],  # any single word
        "s3_endpoint_url": [
            r"(https?:\/\/)?([a-zA-Z0-9_-]+\.)+[a-zA-Z0-9_-]+(:[0-9]+)?\/?"
        ],  # "https://XXX.(XXX.)*n.XXX:XXXX/",
        "s3_bucket": [r"[a-zA-Z0-9_-]+"],  # any single word
        "pfs_prefix_path": [
            r"\/([a-zA-Z0-9_-]+\/?)+"
        ],  # any word sequence (no .) delimited by slashes, starting with /
        "omp_num_threads": [r"[0-9]+"],  # any number,
        "mpi_np": [r"[0-9]+"],  # any number,
        "modules": [r"\[('([a-zA-Z0-9_.-]+\/?)+',? ?)+\]"],  # list of module names, delmited by commas
        #################
        # config_server #
        #################
        "user": [r"[a-zA-Z0-9_]+"],  # any single word (word: character sequence containing alphanumerics or _)
        "host": [
            r"[a-zA-Z0-9_\.-]+[a-zA-Z0-9_-]+"
        ],  # any word sequence (with - and _) optionally delimited by dots, but not ending with one
        "venv_path": [r"^(~)?\/([a-zA-Z0-9_.-]+\/?)+"],  # any word sequence delimited by slashes, can start with ~ or /
        "ssh_key": [r"^(~)?\/([a-zA-Z0-9_.-]+\/?)+"],  # any word sequence delimited by slashes, can start with ~ or /
        "compute_partition": [r"[a-zA-Z0-9_]+"],  # any single word,
        "upload_partition": [r"[a-zA-Z0-9_]+"],  # any single word
        "account": [r"[a-zA-Z0-9_]+"],  # any single word
        "qos": [r"[a-zA-Z0-9_]+"],  # any single word
        "mail": [r"[a-zA-Z0-9_\.]+@[a-zA-Z0-9_\.]+"],  # any valid email type (no dashes or pluses)
        "walltime": [r"([0-9]+-)?([0-9]+:)?([0-9]+:)?[0-9]+"],  # DD-HH:MM:SS
        "nodes": [r"[0-9]+(k|m)?"],  # any number, possibly ending with k or m
        "tasks_per_node": [r"[0-9]+"],  # any number
        "cpus_per_task": [r"[0-9]+"],  # any number
        "debug": [r"[a-zA-Z0-9_]+"],  # any single word
    }

    # make sure keyword values match the expected format
    for key, value in dictionary.items():
        for pattern in keyword_formats[key]:
            match = re.fullmatch(pattern, str(value))
            if match:
                break
        else:
            raise SyntaxError(f"Unexpected format for keyword '{key}': {value}")


class UserInput:
    """Class containing command-line input arguments passed as JSON-formatted dictionary

    Attributes
    ----------
    id : str
        unique id of the run (preferably of the UUID.hex type)
    sql_query : str
        SQL query
    script_path : str
        path to the Python script with the analysis on the files returned by the SQL query
    container_path : str
        path to the Singularity container provided by the user
    container_url : str
        URL to the Docker/Singularity container provided by the user
    exec_command : str
        command to be launched within the container (with its own options and flags if needed)
    config_hpc : dict
        dictionary with custom configuration options for hpc version
    config_server : dict
        dictionary with custom configuration options for server version
    """

    def __init__(self, data: dict[str, str]) -> None:
        """_summary_

        Args:
            data (dict[str, str]): dictionary with the user input (id, sql_query, script, config)
        """
        logger.debug(f"Received input dict: {data}")

        self.id = data["id"]
        self.sql_query = data["sql_query"]

        try:
            self.script_path = data["script_path"]
        except KeyError:  # no script provided
            self.script_path = None

        try:
            self.container_path = data["container_path"]
        except KeyError:  # no container provided
            self.container_path = None

        try:
            self.container_url = data["container_url"]
        except KeyError:  # no container provided
            self.container_url = None

        try:
            self.exec_command = data["exec_command"]
        except KeyError:  # no script provided
            self.exec_command = None

        try:
            self.config_hpc = json.loads(data["config_hpc"].replace("'", '"'))
        except KeyError:  # no custom config provided
            self.config_hpc = None
        except AttributeError:  # config was initialized manually
            self.config_hpc = data["config_hpc"]

        try:
            self.config_server = json.loads(data["config_server"].replace("'", '"'))
        except KeyError:  # no custom config provided
            self.config_server = None
        except AttributeError:  # config was initialized manually
            self.config_server = data["config_server"]

        logger.debug(f"UserInput.id: {self.id}")
        logger.debug(f"UserInput.sql_query: {self.sql_query}")
        logger.debug(f"UserInput.script_path: {self.script_path}")
        logger.debug(f"UserInput.container_path: {self.container_path}")
        logger.debug(f"UserInput.container_url: {self.container_url}")
        logger.debug(f"UserInput.exec_command: {self.exec_command}")
        logger.debug(f"UserInput.config_hpc: {self.config_hpc}")
        logger.debug(f"UserInput.config_server: {self.config_server}")

    @classmethod
    def from_cli(cls):
        """Class constructor from command-line argument.
        Expects a properly JSON-formatted dictionary as command line argument.

        Returns
        -------
        UserInput
            UserInput instance
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
            UserInput instance

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

    @classmethod
    def from_dict(cls, json_dict: str):
        """Class constructor from dictionary.
        Expects a JSON-formatted dictionary as command line argument

        Parameters
        ----------
        json_dict : str
            JSON-formatted dictionary with the input info

        Returns
        -------
        UserInput
            UserInput instance

        """

        data = json.loads(json_dict)
        logger.info(f"Received input from JSON file: {data}")

        return cls(data)


class Config:
    """Class containing configuration info for hpc/server.
    At initialization, it reads and loads the default etc/default/config_{hpc/server}.json file.
    Settings can be overwritten by passing a dictionary to the load_custom_config method
    """

    def __init__(self, version: str) -> None:
        """Initialization for Config class

        Parameters
        ----------
        version : str
            specifies whether the config is relative to the hpc or server version

        Raises
        ------
        NameError
            If version is not 'hpc' or 'server' raises an error
        """
        if version not in ["hpc", "server"]:
            raise NameError("config file only available for 'hpc' or 'server'")

        self.version = version

        default = self.load_default_config(version=version)
        for key, value in default.items():
            setattr(self, key, value)

        sanitize_dictionary(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def load_default_config(self, version: str) -> dict[str, str]:
        """Load default configuration as found in /etc/default.
        If present, overwrites these defaults with the contents of ~/.config/dlaas/config_<version>.json.

        Parameters
        ----------
        version : str
            specify whether the configuration is for hpc/server version

        Returns
        -------
        dict[str, str]
            dictionary with the configuration info
        """
        with open(f"{os.path.dirname(__file__)}/../etc/default/config_{version}.json", "r") as f:
            base_config: dict[str, str] = json.load(f)

        if os.path.exists(f"{os.environ['HOME']}/.config/dlaas/config_{version}.json"):
            with open(f"{os.environ['HOME']}/.config/dlaas/config_{version}.json", "r") as f:
                config: dict[str, str] = json.load(f)

            for key in config:
                if key not in base_config:
                    raise KeyError(f"Unknown parameter in configuration file: '{key}'")

            base_config.update(config)

        sanitize_dictionary(base_config)

        return base_config

    def load_custom_config(self, custom_config: dict[str, str]):
        """Overwrites the default configurations with custom options

        Parameters
        ----------
        custom_config : dict[str, str]
            dictionary containing the settings to overwrite
        """
        for key in custom_config:
            if key not in self.__dict__:
                raise KeyError(f"Unknown parameter in custom configuration: '{key}'")
        self.__dict__.update(custom_config)

        sanitize_dictionary(self.__dict__)
