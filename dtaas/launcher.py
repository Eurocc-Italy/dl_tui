"""
Functions and utilities to interface with HPC (G100) from the VM

Author: @lbabetto
"""

import os
import subprocess
from pymongo import MongoClient
import argparse


import logging

logger = logging.getLogger(__name__)


# Temporary, will need to be replaced with chain user and relative home directory

# HPC
HPC_USER = "lbabetto"
HPC_HOST = "g100"

# MongoDB
VM_SUBMIT_DIR = os.getcwd()
DB_USER = "user"
DB_PASSWORD = "passwd"
DB_IP = "131.175.207.101"
DB_PORT = "27017"

MONGODB_URI = f"mongodb://{DB_USER}:{DB_PASSWORD}@{DB_IP}:{DB_PORT}/"
CLIENT = MongoClient(MONGODB_URI)


def launch_job():
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter"""

    REPO_DIR = "/g100/home/userinternal/lbabetto/REPOS/DTaaS_TUI/dtaas/"
    cmd = f"mkdir dtaas_tui_tests; \
        mkdir dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
        cd dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
        cp {REPO_DIR}slurm.sh .; \
        scp vm:{VM_SUBMIT_DIR}/* .;\
        sbatch slurm.sh"

    logger.debug("Launching command via ssh:")
    logger.debug(cmd)

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"ssh -i /home/centos/.ssh/luca-hpc {HPC_USER}@{HPC_HOST} '{cmd}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    if "Submitted batch job" not in stdout:
        raise RuntimeError("Something gone wrong, job was not launched.")


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler("dtaas.log")

    # Create formatters and add it to handlers
    c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.DEBUG)
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

    # Parsing API input, requires --query keyword, with optional --script
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--script", type=str, required=False)
    args = parser.parse_args()
    logger.debug(f"API input: {args}")

    logger.info(f"User query: {args.query}")
    with open("QUERY", "w") as f:
        f.write(args.query)

    if args.script != "":
        logger.info("User-provided Python script found.")
        with open("script.py", "w") as f:
            f.write(args.script)

    launch_job()
