#!/usr/bin/env python
"""
Module to interface with HPC (G100) from the VM. The purpose of this module is to take the user query and script,
send a HPC job on G100 which calls the wrapper.py on the compute nodes and runs the script on the query results.

Author: @lbabetto
"""

import re
import subprocess
from typing import Tuple
from tuilib.common import Config, UserInput

import logging

logger = logging.getLogger(__name__)


def launch_job(config: Config, user_input: UserInput) -> Tuple[str, str]:
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter

    Parameters
    ----------
    config : Config
        configuration instance with all the relevant data for the job (server)
    user_input : UserInput
        class instance containing the relevant user input (query, script path)

    Returns
    -------
    Tuple[str, str]
        stdout and stderr provided by the SSH command

    Raises
    ------
    RuntimeError
        if "Submitted batch job" is not found in stdout, meaning the job was not launched
    """

    logger.debug(f"Received query: {user_input.query}")

    if user_input.script:
        logger.debug(f"Received script: \n{user_input.script}")

    # SLURM parameters
    partition = config.partition
    account = config.account
    walltime = config.walltime
    nodes = config.nodes
    workdir = config.workdir
    ssh_key = config.ssh_key

    ### Generating job command
    # loading modules
    wrap_cmd = "module load python; "
    # sourcing virtual environment
    wrap_cmd += f"source {config.venv_path}; "
    # calling client version of TUI with common arguments
    wrap_cmd += f"""dtaas_tui_client {{\"ID\": {user_input.id}, \"query\": \"{user_input.query}\""""
    # adding script, if one was provided
    if user_input.script:
        # script = (
        #     user_input.script.replace("\n", "\\n")
        #     .replace("'", "\\'")
        #     .replace('"', '\\"')
        # )
        script = re.escape(user_input)
        wrap_cmd += f""", \"script\": \"{script}\""""
    # closing JSON-formatted string and adding a finalizer file once the job is done
    wrap_cmd += "}; touch JOB_DONE"

    # bash commands to be run via ssh
    ssh_cmd = f"mkdir {user_input.id}; "
    ssh_cmd += f"cd {user_input.id} "
    ssh_cmd += f"sbatch -p {partition} -A {account} -t {walltime} -N {nodes} --ntasks-per-node 48 --wrap '{wrap_cmd}'"

    # TODO: implement ssh via chain user
    full_ssh_cmd = (
        f'ssh -i ~/.ssh/luca-g100 {config["user"]}@{config["host"]} "{ssh_cmd}"'
    )

    logger.debug(f"Launching command via ssh: {ssh_cmd}")
    logger.debug(f"Full ssh command: {full_ssh_cmd}")

    stdout, stderr = subprocess.Popen(
        full_ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"stdout: {stdout}")
    logger.debug(f"stderr: {stderr}")

    if "Submitted batch job" not in stdout:
        raise RuntimeError(
            f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}"
        )

    return stdout, stderr
