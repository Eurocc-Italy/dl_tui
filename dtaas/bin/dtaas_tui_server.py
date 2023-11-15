#!/usr/bin/env python
"""
Module to interface with HPC (G100) from the VM. The purpose of this module is to take the user query and script,
send a HPC job on G100 which calls the client program on the compute nodes and runs the script on the query results.

Author: @lbabetto
"""

import sys
from dtaas.tuilib.server import create_remote_directory, copy_json_input, copy_user_script, launch_job


def main():
    """Server-side (VM) version of the DTaaS TUI"""
    json_path = sys.argv[1]

    create_remote_directory(json_path=json_path)
    copy_json_input(json_path=json_path)
    copy_user_script(json_path=json_path)
    launch_job(json_path=json_path)


if __name__ == "__main__":
    # setting up logging
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("server.log", mode="w")
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # running main function
    main()
