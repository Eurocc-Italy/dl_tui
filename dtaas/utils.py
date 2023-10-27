"""
Various utilities used throughout the code

Author: @lbabetto
"""

import os
import json


def load_config():
    # read default configuration file
    with open(f"{os.path.dirname(__file__)}/config.json", "r") as f:
        config = json.load(f)
    # read custom configuration file, if present
    if os.path.exists("config.json"):
        with open(f"config.json", "r") as f:
            new_config = json.load(f)
        for key in new_config:
            config[key].update(new_config[key])
    return config
