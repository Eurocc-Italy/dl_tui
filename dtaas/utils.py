"""
Various utilities used throughout the code

Author: @lbabetto
"""

import os
import json
import argparse
from typing import Dict, Tuple
from io import StringIO


def load_config() -> Dict[str, str]:
    """Reads and loads the default config.json file in /etc/default,
    and then (if present) loads the custom config.json file in /etc overwriting default values

    Returns
    -------
    Dict[str, str]
        configuration dictionary
    """
    # read default configuration file
    with open(f"{os.path.dirname(__file__)}/../etc/default/config.json", "r") as f:
        config = json.load(f)
    # read custom configuration file, if present
    if os.path.exists(f"{os.path.dirname(__file__)}/../etc/config.json"):
        with open(f"{os.path.dirname(__file__)}/../etc/config.json", "r") as f:
            new_config = json.load(f)
        for key in new_config:
            config[key].update(new_config[key])
    return config


def parse_cli_input() -> Tuple[str, str]:
    """Parses command line input, requires --query keyword, with optional --script

    Returns
    -------
    str, str
        paths to the files containing the SQL query and Python script
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--script", type=str, required=False)
    args = parser.parse_args()

    return args.query, args.script
