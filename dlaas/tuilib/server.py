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
from dlaas.tuilib.common import Config, UserInput


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
    logger.info(f"Creating remote directory: {user_input.id}")
    logger.debug(f"Full user input: {user_input.__dict__}")

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
    logger.info(f"Copying user input in JSON form to HPC")
    logger.debug(f"Full user input: {user_input.__dict__}")

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


def copy_user_executable(json_path: str):
    """Copy user executable file to remote machine (if present in json file)
    Executable can either be a Python script or a Singularity container

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
    logger.info(f"Copying user script to remote directory")
    logger.debug(f"Full user input: {user_input.__dict__}")

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    if user_input.script_path:
        logger.debug(f"Python script: \n{user_input.script_path}")
        ssh_cmd = f"scp -i {config.ssh_key} {user_input.script_path} {config.user}@{config.host}:~/{user_input.id}/{basename(user_input.script_path)}"

    elif user_input.container_path:
        logger.debug(f"Container path: \n{user_input.container_path}")
        ssh_cmd = f"scp -i {config.ssh_key} {user_input.container_path} {config.user}@{config.host}:~/{user_input.id}/{basename(user_input.container_path)}"

    else:
        return "", ""

    logger.debug(f"launching command: {ssh_cmd}")
    stdout, stderr = subprocess.Popen(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"scp stdout: {stdout}")
    logger.debug(f"scp stderr: {stderr}")

    return stdout, stderr


def launch_job(json_path: str):
    """Launch job on HPC (either a Python script or a Singularity container)

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
    logger.info(f"Launching job on HPC")
    logger.debug(f"Full user input: {user_input.__dict__}")

    logger.info(f"Running SQL query: {user_input.sql_query}")

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    logger.debug(f"Server config: {config.__dict__}")

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
    # NOTE: it is probably not necessary to source the environment as the executable can be ran safely via the
    # `{config.venv_path}/bin/dl_tui_hpc` call. Still, it is safer to do so if a custom python script is provided,
    # since we are sure the correct libraries will be available to the executable
    wrap_cmd = f"module load python; "
    wrap_cmd += f"source {config.venv_path}/bin/activate; "
    wrap_cmd += f"dl_tui_hpc {basename(json_path)}; "
    wrap_cmd += "touch JOB_DONE"

    # Generating SSH command
    ssh_cmd = f"cd {user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} --qos {qos} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t {walltime} -N {nodes} "
    ssh_cmd += f"--ntasks-per-node {ntasks_per_node} "
    ssh_cmd += f"--wrap '{wrap_cmd}'"

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

    try:
        slurm_job_id = int(stdout.lstrip("Submitted batch job "))
        logger.info(f"Launched job on HPC with ID: {slurm_job_id}")
    except ValueError:  # exception is raised during conversion of empty string to int
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    return stdout, stderr, slurm_job_id


def upload_results(json_path: str, slurm_job_id: int):
    """Upload results of completed job to S3 via the Python script created by the HPC version.

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
    logger.info(f"Uploading results to S3 and MongoDB")
    logger.debug(f"Full user input: {user_input.__dict__}")

    # loading server config
    config = Config("server")
    if user_input.config_server:
        config.load_custom_config(user_input.config_server)

    logger.debug(f"Server config: {config.__dict__}")

    # SLURM parameters
    partition = config.upload_partition
    account = config.account
    mail = config.mail
    ssh_key = config.ssh_key

    # Creating wrap command to be passed to sbatch
    wrap_cmd = f"module load python; "  # TODO: placeholder for G100, as Python is not available by default.
    wrap_cmd += f"source {config.venv_path}/bin/activate; "
    wrap_cmd += f"cd run_job_*; "  # if a script/container was also provided
    wrap_cmd += f"python upload_results_{user_input.id}.py; "
    wrap_cmd += "touch RESULTS_UPLOADED; "
    # wrap_cmd += f"rm -rf ../{user_input.id}; "  # remove temporary directory from HPC. Comment this line for debugging
    # wrap_cmd += f"rm -rf ../../{user_input.id}"  # remove temporary directory from HPC. Comment this line for debugging

    # Generating SSH command
    ssh_cmd = f"cd {user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t 00:10:00 "
    ssh_cmd += f"-d afterok:{slurm_job_id} "
    ssh_cmd += f"--wrap '{wrap_cmd}'"

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

    logger.info(f"Results are available on S3 with the key: results_{user_input.id}.zip")
    logger.info(f'Results are available on MongoDB with the key: "job_id": {user_input.id}')

    if "Submitted batch job" not in stdout:
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    return stdout, stderr
