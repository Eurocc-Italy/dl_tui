"""
Custom functions for the Bologna Digital Twin implementation of the Data Lake

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import subprocess
from tuilib.common import UserInput, Config
from typing import Tuple


def download_input_files(json_path: str) -> Tuple[str, str, str]:
    """Launch a HPC job which downloads the input files to the work directory. It is expected that a file named
    input.json exists and contains the <filename>: <URL> entries to download files to HPC

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input

    Returns
    -------
    Tuple[str, str, str]
        stdout and stderr of the ssh command + Slurm job ID for build job

    Raises
    ------
    RuntimeError
        if "Submitted batch job" is not found in stdout, meaning the job was not launched
    """

    ###########################################################################
    #                         Parse input and config                          #
    ###########################################################################

    user_input = UserInput.from_json(json_path=json_path)
    logger.info(f"Downloading input files to HPC")
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

    ###########################################################################
    #                         Set up download script                          #
    ###########################################################################

    # Create download script to be copied to HPC
    with open(f"download_input_{user_input.id}.py", "w") as f:
        content = "import requests, json"
        content += "with open('input.json', 'r') as f:"
        content += "    files_dict = json.load(f)"
        content += "for filename, url in files_dict.items():"
        content += "    response = requests.get(url)"
        content += r"    with open('input/{filename}', 'wb') as f:"
        content += "        f.write(response.content)"
        f.write(content)

    # Copy download script and input json
    ssh_cmd = f"scp -i {config.ssh_key} input.json download_input_{user_input.id}.py {config.user}@{config.host}:~/{user_input.id}/"
    logger.debug(f"launching command: {ssh_cmd}")
    stdout, stderr = subprocess.Popen(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"scp download script input stdout: {stdout}")
    logger.debug(f"scp download script input stderr: {stderr}")

    ###########################################################################
    #                             Launch HPC job                              #
    ###########################################################################

    # Creating wrap command to be passed to sbatch
    wrap_cmd = f"module load python; "  # TODO: placeholder for G100, as Python is not available by default.
    wrap_cmd += f"source {config.venv_path}/bin/activate; "
    wrap_cmd += f"python download_input_{user_input.id}.py; "

    # Generating SSH command
    ssh_cmd = f"cd {user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t 01:00:00 "
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

    ###########################################################################
    #                           Check stdout/err                              #
    ###########################################################################

    try:
        slurm_job_id = int(stdout.lstrip("Submitted batch job "))
        logger.info(f"Downloading input files to HPC. Job ID | Slurm ID: {user_input.id} | {slurm_job_id}")
    except ValueError:  # exception is raised during conversion of empty string to int
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    if "Submitted batch job" not in stdout:
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    return stdout, stderr, slurm_job_id
