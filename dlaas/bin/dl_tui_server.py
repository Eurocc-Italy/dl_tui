#!/usr/bin/env python
"""
Module to interface with HPC from the VM running the metadata database. The purpose of this module is to take the 
user query and script, send a HPC job which calls the HPC executable on the compute nodes and runs the script 
on the query results.

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

import sys
from dlaas.tuilib.server import create_remote_directory, copy_json_input, copy_user_script, launch_job, upload_results


def main():
    """Executable intended to run on the server VM"""
    json_path = sys.argv[1]

    create_remote_directory(json_path=json_path)
    copy_json_input(json_path=json_path)
    copy_user_script(json_path=json_path)
    stdout, stderr, slurm_job_id = launch_job(json_path=json_path)
    upload_results(json_path=json_path, slurm_job_id=slurm_job_id)


if __name__ == "__main__":
    main()