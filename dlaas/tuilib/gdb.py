"""
Custom functions for the Bologna Digital Twin implementation of the Data Lake

Author: @lbabetto
"""

import logging

logger = logging.getLogger(__name__)

import subprocess
from dlaas.tuilib.common import UserInput, Config
from typing import Tuple


def download_input_files(json_path: str, build_job_id: str = None) -> Tuple[str, str, str]:
    """Launch a HPC job which downloads the input files to the work directory. It is expected that a file named
    input.json exists and contains the <filename>: <URL> entries to download files to HPC

    Parameters
    ----------
    json_path : str
        Path to the JSON file with the user input
    build_job_id : int, optional
        Slurm job ID used for building a remote container

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
        content = "import requests, json\n"
        content += "with open('input.json', 'r') as f:\n"
        content += "    files_dict = json.load(f)\n"
        content += "for filename, url in files_dict.items():\n"
        content += "    response = requests.get(url)\n"
        content += "    with open(f'input/{filename}', 'wb') as f:\n"
        content += "        f.write(response.content)\n"
        f.write(content)

    # Create input and output folders
    ssh_cmd = f"ssh -i {config.ssh_key} {config.user}@{config.host} 'mkdir $SCRATCH/{user_input.id}/input $SCRATCH/{user_input.id}/output'"
    logger.debug(f"launching command: {ssh_cmd}")
    stdout, stderr = subprocess.Popen(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"scp download container input stdout: {stdout}")
    logger.debug(f"scp download container input stderr: {stderr}")

    # Copy download script and input json
    ssh_cmd = f"scp -i {config.ssh_key} input.json download_input_{user_input.id}.py {config.user}@{config.host}:\$SCRATCH/{user_input.id}/"
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
    ssh_cmd = f"cd \$SCRATCH/{user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t 01:00:00 "
    if build_job_id:
        ssh_cmd += f"-d afterok:{build_job_id} "
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


def upload_results(json_path: str, slurm_job_id: int) -> Tuple[str, str]:
    """Launch a HPC job which uploads the output files to the S3 server. It is expected that a file named
    output.json exists and contains the <filename>: <URL> entries to upload files from HPC

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
    #                         Set up upload script                          #
    ###########################################################################

    # Create upload script to be copied to HPC
    with open(f"upload_output_{user_input.id}.py", "w") as f:
        content = "import requests, json\n"
        content += "with open('output.json', 'r') as f:\n"
        content += "    files_dict = json.load(f)\n"
        content += "for filename, presigned_data in files_dict.items():\n"
        content += '    files = {"file": (filename, open(f"./output/{filename}", "rb"), None)}\n'
        content += "    response = requests.post(presigned_data['url'], data=presigned_data['fields'], files=files)\n"
        f.write(content)

    # Copy download script and input json
    ssh_cmd = f"scp -i {config.ssh_key} output.json upload_output_{user_input.id}.py {config.user}@{config.host}:\$SCRATCH/{user_input.id}/"
    logger.debug(f"launching command: {ssh_cmd}")
    stdout, stderr = subprocess.Popen(
        ssh_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()
    stdout = str(stdout, encoding="utf-8")
    stderr = str(stderr, encoding="utf-8")
    logger.debug(f"scp upload container output stdout: {stdout}")
    logger.debug(f"scp upload container output stderr: {stderr}")

    ###########################################################################
    #                             Launch HPC job                              #
    ###########################################################################

    # Creating wrap command to be passed to sbatch
    wrap_cmd = f"module load python; "  # TODO: placeholder for G100, as Python is not available by default.
    wrap_cmd += f"source {config.venv_path}/bin/activate; "
    wrap_cmd += f"python upload_output_{user_input.id}.py; "
    wrap_cmd += "touch RESULTS_UPLOADED; "
    wrap_cmd += (
        f"rm -rf ../{user_input.id}; "  # remove temporary directory from HPC. NOTE: Comment this line for debugging
    )

    # Generating SSH command
    ssh_cmd = f"cd \$SCRATCH/{user_input.id}; "
    ssh_cmd += f"sbatch -p {partition} -A {account} "
    ssh_cmd += f"--mail-type ALL --mail-user {mail} "
    ssh_cmd += f"-t 01:00:00 "
    if slurm_job_id:
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

    ###########################################################################
    #                           Check stdout/err                              #
    ###########################################################################

    try:
        slurm_job_id = int(stdout.lstrip("Submitted batch job "))
        logger.info(f"Uploading results to remote server. Job ID | Slurm ID: {user_input.id} | {slurm_job_id}")
    except ValueError:  # exception is raised during conversion of empty string to int
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    if "Submitted batch job" not in stdout:
        raise RuntimeError(f"Something gone wrong, job was not launched.\nstdout: {stdout}\nstderr: {stderr}")

    return stdout, stderr