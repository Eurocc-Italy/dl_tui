"""
Functions to interface with HPC (G100) from the VM

Author: @lbabetto
"""

import os
import subprocess
import argparse
from dtaas import utils

config = utils.load_config()

import logging

logging.basicConfig(
    # filename=config["LOGGING"]["logfile"],
    format=config["LOGGING"]["format"],
    level=config["LOGGING"]["level"].upper(),
)


def launch_job(query, script):
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter

    Parameters
    ----------
    query : str
        SQL query to be passed to the wrapper on HPC
    script : str
        Python script for the processing of the query results
    """

    # SLURM parameters
    partition = config["HPC"]["partition"]
    account = config["HPC"]["account"]
    walltime = config["HPC"]["walltime"]
    nodes = config["HPC"]["nodes"]
    wrap_cmd = f'module load python; \
source {config["HPC"]["venv_path"]}; \
python {config["HPC"]["repo_dir"]}/wrapper.py --query """{query}""" --script """{script}"""'

    # bash commands to be run via ssh; TODO: decide structure of temporary folders
    ssh_cmd = f"mkdir dtaas_tui_tests; \
mkdir dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
scp vm:{os.getcwd()}/config.json .; \
cd dtaas_tui_tests/{os.path.basename(os.getcwd())}; \
sbatch -p {partition} -A {account} -t {walltime} -N {nodes} --ntasks-per-node 48 --wrap '{wrap_cmd}'"

    logging.debug(f"Launching command via ssh: {ssh_cmd}")

    stdout, stderr = subprocess.Popen(
        # TODO: key currently necessary, will be removed when we switch to chain user
        f'ssh -i /home/centos/.ssh/luca-hpc {config["HPC"]["user"]}@{config["HPC"]["host"]} "{ssh_cmd}"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    logging.debug(f"stdout: {stdout}")
    logging.debug(f"stderr: {stderr}")

    if "Submitted batch job" not in str(stdout, encoding="utf-8"):
        raise RuntimeError("Something gone wrong, job was not launched.")


if __name__ == "__main__":
    # Parsing API input, requires --query keyword, with optional --script
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--script", type=str, required=False)
    args = parser.parse_args()
    logging.debug(f"API input (launcher): {args}")
    exit()
    launch_job(query=args.query, script=args.script)
