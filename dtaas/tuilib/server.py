#!/usr/bin/env python
"""
Module to interface with HPC (G100) from the VM. The purpose of this module is to take the user query and script,
send a HPC job on G100 which calls the client program on the compute nodes and runs the script on the query results.

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

from os.path import basename
import subprocess
from typing import Tuple
from dtaas.tuilib.common import Config, UserInput


def create_remote_directory(json_path: str) -> Tuple[str, str]:
    """Create remote temporary directory on HPC

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input

    Returns
    -------
    Tuple[str, str]
        stdout and stderr of the ssh command
    """

    user_input = UserInput.from_json(json_path=json_path)

    if user_input.script_path:
        logger.debug(f"Script name: \n{user_input.script_path}")

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    ssh_cmd = f"ssh -i {config.ssh_key} {config.user}@{config.host} 'mkdir {user_input.id}'"
    logger.debug(f"launching command: {ssh_cmd}")
    stdout, stderr = subprocess.Popen(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"mkdir stdout: {stdout}")
    logger.debug(f"mkdir stderr: {stderr}")

    if "mkdir: cannot create directory" in stderr:
        raise RuntimeError("Directory already present, cannot continue.")

    return stdout, stderr


def copy_json_input(json_path: str):
    """Copy .json file with the user input to remote machine

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input

    Returns
    -------
    Tuple[str, str]
        stdout and stderr of the ssh command
    """

    user_input = UserInput.from_json(json_path=json_path)

    if user_input.script_path:
        logger.debug(f"Script name: \n{user_input.script_path}")

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    # copying input JSON
    ssh_cmd = f"scp -i {config.ssh_key} {json_path} {config.user}@{config.host}:~/{user_input.id}/{basename(json_path)}"
    logger.debug(f"launching command: {ssh_cmd}")
    stdout, stderr = subprocess.Popen(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"scp json input stdout: {stdout}")
    logger.debug(f"scp json input stderr: {stderr}")

    return stdout, stderr


def copy_user_script(json_path: str):
    """Copy user script file to remote machine (if present in json file)

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input

    Returns
    -------
    Tuple[str, str]
        stdout and stderr of the ssh command
    """

    user_input = UserInput.from_json(json_path=json_path)

    if user_input.script_path:
        logger.debug(f"Script name: \n{user_input.script_path}")

        # loading server config
        config = Config("server")
        if user_input.config_server:
            config.load_custom_config(user_input.config_server)

        ssh_cmd = f"scp -i {config.ssh_key} {user_input.script_path} {config.user}@{config.host}:~/{user_input.id}/{basename(user_input.script_path)}"
        logger.debug(f"launching command: {ssh_cmd}")
        stdout, stderr = subprocess.Popen(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).communicate()
        stdout = str(stdout, encoding="utf-8")
        stderr = str(stderr, encoding="utf-8")
        logger.debug(f"scp user script stdout: {stdout}")
        logger.debug(f"scp user script stderr: {stderr}")

        return stdout, stderr

    else:
        return "", ""


def launch_job(json_path: str):
    """Launch job on HPC

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input

    Returns
    -------
    Tuple[str, str, str]
        stdout and stderr provided by the SSH command + SLURM job identifier (necessary for upload dependency)

    Raises
    ------
    RuntimeError
        if "Submitted batch job" is not found in stdout, meaning the job was not launched
    """

    user_input = UserInput.from_json(json_path=json_path)

    logger.debug(f"Received SQL query: {user_input.sql_query}")

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    # SLURM parameters
    partition = config.compute_partition
    qos = config.qos
    account = config.account
    walltime = config.walltime
    mail = config.mail
    nodes = config.nodes
    ntasks_per_node = config.ntasks_per_node
    ssh_key = config.ssh_key

    # Creating wrap command to be passed to sbatch
    wrap_cmd = f"source {config.venv_path}/bin/activate; "
    wrap_cmd += f"dtaas_tui_client {basename(json_path)}; "
    wrap_cmd += "touch JOB_DONE"

    # Generating SSH command
    ssh_cmd = f"cd {user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} --qos {qos} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t {walltime} -N {nodes} "
    ssh_cmd += f"--ntasks-per-node {ntasks_per_node} "
    ssh_cmd += f"--wrap '{wrap_cmd}'"

    # TODO: implement ssh via chain user
    full_ssh_cmd = rf'ssh -i {ssh_key} {config.user}@{config.host} "{ssh_cmd}"'

    logger.debug(f"Launching command via ssh:\n{ssh_cmd}")
    logger.debug(f"Full ssh command:\n{full_ssh_cmd}")

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

    slurm_job_id = int(stdout.lstrip("Submitted batch job "))

    if "Submitted batch job" not in stdout:
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    return stdout, stderr, slurm_job_id


def upload_results(json_path: str, slurm_job_id: int):
    """Upload results of completed job to S3 via the Python script created by the client version.

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input
    slurm_job_id : int
        Slurm job ID for the calculation of which to save the data


    Returns
    -------
    Tuple[str, str]
        stdout and stderr provided by the SSH command

    Raises
    ------
    RuntimeError
        if "Submitted batch job" is not found in stdout, meaning the job was not launched
    """

    user_input = UserInput.from_json(json_path=json_path)

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    # SLURM parameters
    partition = config.upload_partition
    account = config.account
    mail = config.mail
    ssh_key = config.ssh_key

    # Creating wrap command to be passed to sbatch
    wrap_cmd = f"source {config.venv_path}/bin/activate; "
    wrap_cmd += f"python upload_results_{user_input.id}.py"

    # Generating SSH command
    ssh_cmd = f"cd {user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t 00:10:00 "
    ssh_cmd += f"-d afterok:{slurm_job_id} "
    ssh_cmd += f"--wrap '{wrap_cmd}'"

    # TODO: implement ssh via chain user
    full_ssh_cmd = rf'ssh -i {ssh_key} {config.user}@{config.host} "{ssh_cmd}"'

    logger.debug(f"Launching command via ssh:\n{ssh_cmd}")
    logger.debug(f"Full ssh command:\n{full_ssh_cmd}")

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
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    return stdout, stderr
