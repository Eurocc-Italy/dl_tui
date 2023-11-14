#!/usr/bin/env python
"""
Module to interface with HPC (G100) from the VM. The purpose of this module is to take the user query and script,
send a HPC job on G100 which calls the client program on the compute nodes and runs the script on the query results.

Author: @lbabetto
"""

# setting up logging
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("server.log", mode="w")
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

from dtaas.tuilib.common import Config, UserInput
from dtaas.tuilib.server import launch_job


def main():
    # loading config
    config = Config(version="server")
    logger.debug(config)

    # running query and script
    user_input = UserInput.from_cli()

    # Launching job
    launch_job(
        config=config,
        user_input=user_input,
    )
