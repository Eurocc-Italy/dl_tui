"""
Functions to interface with HPC (G100) from the VM

Author: @lbabetto
"""

import os
import subprocess
from typing import Dict
import argparse
from dtaas.utils import load_config

import logging

logger = logging.getLogger(__name__)


def launch_job(hpc_config: Dict[str, str], query: str, script: str):
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter

    Parameters
    ----------
    hpc_config : Dict[str, str]
        dictionary with the relevant data for HPC ( = config["HPC"] dict)
    query : str
        SQL query to be passed to the wrapper on HPC
    script : str
        Python script for the processing of the query results
    """

    logger.debug(f"Received query: {query}")
    if script:
        logger.debug(f"Received script: \n{script}")

    # SLURM parameters
    partition = hpc_config["partition"]
    account = hpc_config["account"]
    walltime = hpc_config["walltime"]
    nodes = hpc_config["nodes"]
    wrap_cmd = f'module load python; \
source {hpc_config["venv_path"]}; \
python {hpc_config["repo_dir"]}/wrapper.py --query """{query}""" --script """{script}"""'

    # bash commands to be run via ssh; TODO: decide structure of temporary folders
    ssh_cmd = f"mkdir dtaas_tui_tests; \
mkdir dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
cd dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
sbatch -p {partition} -A {account} -t {walltime} -N {nodes} --ntasks-per-node 48 --wrap '{wrap_cmd}'"

    full_ssh_cmd = f'ssh -i /home/centos/.ssh/luca-hpc {hpc_config["user"]}@{hpc_config["host"]} """{ssh_cmd}"""'
    logger.debug(f"Launching command via ssh: {ssh_cmd}")
    logger.debug(f"Full ssh command: {full_ssh_cmd}")

    stdout, stderr = subprocess.Popen(
        # TODO: key currently necessary, will be removed when we switch to chain user
        full_ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    logger.debug(f"stdout: {str(stdout, encoding='utf-8')}")
    logger.debug(f"stderr: {str(stderr, encoding='utf-8')}")

    if "Submitted batch job" not in str(stdout, encoding="utf-8"):
        raise RuntimeError("Something gone wrong, job was not launched.")


if __name__ == "__main__":
    # loading config and setting up URI
    config = load_config()
    mongodb_uri = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"

    # connecting to client
    logger.info(f"Connecting to client: {mongodb_uri}")
    client = MongoClient(mongodb_uri)

    # accessing collection
    logger.info(f"Loading database {config['MONGO']['database']}, collection {config['MONGO']['collection']}")
    collection = client[config["MONGO"]["database"]][
        config["MONGO"]["collection"]
    ]  # Parsing API input, requires --query keyword, with optional --script
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--script", type=str, required=False)
    args = parser.parse_args()
    logger.debug(f"API input (launcher): {args}")
    launch_job(query=args.query, script=args.script)
