"""
Functions to interface with HPC (G100) from the VM

Author: @lbabetto
"""

import os
import subprocess
from pymongo import MongoClient
import argparse
from dtaas import utils

# Needed for dtaas_wrapper
SUBMIT_DIR = os.getcwd()

config = utils.load_config()

import logging

logging.basicConfig(
    filename=config["LOGGING"]["logfile"],
    format=config["LOGGING"]["format"],
    level=config["LOGGING"]["level"].upper(),
)


MONGODB_URI = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"
CLIENT = MongoClient(MONGODB_URI)
logging.info(f"Connected to client: {MONGODB_URI}")


def launch_job():
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter"""

    cmd = f"module load python; \
source {config['GENERAL']['venv_path']}; \
python {config['GENERAL']['repo_dir']}/dtaas_wrapper.py"

    cmd = f"mkdir dtaas_tui_tests; \
export REPO_DIR={config['general']['repo_dir']}; \
mkdir dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
cd dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
cp {config['general']['repo_dir']}slurm.sh .; \
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
