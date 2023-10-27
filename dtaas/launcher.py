"""
Functions and utilities to interface with HPC (G100) from the VM

Author: @lbabetto
"""

import os
import subprocess
from pymongo import MongoClient
import argparse
import json

import logging

logging.basicConfig(filename="logfile.log", format="%(levelname)s: %(message)s", level=logging.DEBUG)

# Needed for dtaas_wrapper
SUBMIT_DIR = os.getcwd()

# read default configuration file
with open(f"{os.path.dirname(__file__)}/config.json", "r") as f:
    config = json.load(f)

# read custom configuration file, if present
if os.path.exists("config.json"):
    with open(f"config.json", "r") as f:
        new_config = json.load(f)
    for key in new_config:
        config[key].update(new_config[key])


MONGODB_URI = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"
CLIENT = MongoClient(MONGODB_URI)


def launch_job():
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter"""

    REPO_DIR = "/g100/home/userinternal/lbabetto/REPOS/DTaaS_TUI/dtaas/"
    cmd = f"mkdir dtaas_tui_tests; \
        mkdir dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
        cd dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
        cp {REPO_DIR}slurm.sh .; \
        scp vm:{SUBMIT_DIR}/* .;\
        sbatch slurm.sh"

    logging.debug("Launching command via ssh:")
    logging.debug(cmd)

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"ssh -i /home/centos/.ssh/luca-hpc {config['HPC']['user']}@{config['HPC']['host']} '{cmd}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    if "Submitted batch job" not in str(stdout, encoding="utf-8"):
        raise RuntimeError("Something gone wrong, job was not launched.")


if __name__ == "__main__":
    # Parsing API input, requires --query keyword, with optional --script
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--script", type=str, required=False)
    args = parser.parse_args()
    logging.debug(f"API input: {args}")

    logging.info(f"User query: {args.query}")
    with open("QUERY", "w") as f:
        f.write(args.query)

    if args.script != "":
        logging.info("User-provided Python script found.")
        with open("script.py", "w") as f:
            f.write(args.script)

    launch_job()
