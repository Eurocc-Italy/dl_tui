"""
Module to interface with HPC (G100) from the VM. The purpose of this module is to take the user query and script,
send a HPC job on G100 which calls the wrapper.py on the compute nodes and runs the script on the query results.

Author: @lbabetto
"""

import os
import subprocess
from typing import Dict
import argparse
from dtaas.utils import load_config
from pymongo import MongoClient

import logging

logger = logging.getLogger(__name__)


def launch_job(config: Dict[str, str], query_path: str, script_path: str):
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter

    Parameters
    ----------
    config : Dict[str, str]
        dictionary with the relevant data for the job (taken from load_config)
    query_path : str
        path to the file containing the SQL query
    script_path : str
        path to the file containing the Python analysis script

    Parameters
    ----------
    config : Dict[str, str]
        dictionary with the relevant data for the job (taken from load_config)
    query_path : str
        path to the file containing the SQL query
    script_path : str
        path to the file containing the Python analysis script

    Raises
    ------
    RuntimeError
        if "Submitted batch job" is not found in stdout, meaning the job was not launched
    """

    with open(query_path, "r") as f:
        logger.debug(f"Received query: {f.read()}")

    if script_path:
        with open(script_path, "r") as f:
            logger.debug(f"Received script: \n{f.read()}")

    # SLURM parameters
    hpc_config = config["HPC"]
    partition = hpc_config["partition"]
    account = hpc_config["account"]
    walltime = hpc_config["walltime"]
    nodes = hpc_config["nodes"]
    workdir = hpc_config["workdir"]
    ssh_key = hpc_config["ssh_key"]

    wrap_cmd = "module load python; "
    wrap_cmd += f'source {hpc_config["venv_path"]}; '
    if script_path:
        wrap_cmd += f'python {hpc_config["repo_dir"]}/wrapper.py --query {query_path} --script {script_path}'
    else:
        wrap_cmd += f'python {hpc_config["repo_dir"]}/wrapper.py --query {query_path}'

    # bash commands to be run via ssh; TODO: decide structure of temporary folders
    ssh_cmd = f"mkdir {workdir}; "
    ssh_cmd += f"mkdir {workdir}/{os.path.basename(os.getcwd())}; "
    ssh_cmd += f"cd {workdir}/{os.path.basename(os.getcwd())}; "
    ssh_cmd += f"scp -i {ssh_key} centos@{config['MONGO']['ip']}:{os.getcwd()}/* .; "
    ssh_cmd += f"sbatch -p {partition} -A {account} -t {walltime} -N {nodes} --ntasks-per-node 48 --wrap '{wrap_cmd}'"

    # TODO: implement ssh via chain user
    full_ssh_cmd = f'ssh -i /home/centos/.ssh/luca-hpc {hpc_config["user"]}@{hpc_config["host"]} "{ssh_cmd}"'

    logger.debug(f"Launching command via ssh: {ssh_cmd}")
    logger.debug(f"Full ssh command: {full_ssh_cmd}")

    stdout, stderr = subprocess.Popen(
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
    db = config["MONGO"]
    mongodb_uri = f"mongodb://{db['user']}:{db['password']}@{db['ip']}:{db['port']}/"

    # connecting to client
    logger.info(f"Connecting to client: {mongodb_uri}")
    client = MongoClient(mongodb_uri)

    # accessing collection
    logger.info(f"Loading database {db['database']}, collection {db['collection']}")
    collection = client[config["MONGO"]["database"]][config["MONGO"]["collection"]]

    # Parsing API input, requires --query keyword, with optional --script
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--script", type=str, required=False)
    args = parser.parse_args()
    logger.debug(f"API input (launcher): {args}")

    # Launching job
    launch_job(
        config=config,
        query_path=args.query,
        script_path=args.script,
    )
