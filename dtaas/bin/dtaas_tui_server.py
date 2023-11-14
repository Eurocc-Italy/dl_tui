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

from dtaas.tuilib.common import UserInput
from dtaas.tuilib.server import launch_job


def main():
    # reading user input
    user_input = UserInput.from_cli()

    # Launching job
    launch_job(
        config_server=user_input.config_server,
        job_id=user_input.id,
        query=user_input.query,
        script=user_input.script,
        config_client=user_input.config_client,
    )


if __name__ == "__main__":
    main()
