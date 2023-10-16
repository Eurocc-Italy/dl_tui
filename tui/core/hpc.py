"""
Functions and utilities to interface with HPC (G100) from the VM

!!! WIP !!!

Author: @lbabetto
"""

from typing import List
import subprocess

import logging

logger = logging.getLogger(__name__)

# Temporary, will need to be replaced with chain user and relative home directory
USER = "lbabetto"
HOST = "login.g100.cineca.it"
WORKDIR = "/g100/home/userinternal/lbabetto/PROJECTS/1-DTaas/3-test"


def copy_files(files: List[str]):
    """Copy relevant files to the work directory on G100

    Parameters
    ----------
    files : List[str]
        list containing the path to the files to copy
    """

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"scp -i /home/centos/.ssh/luca-hpc {' '.join(str(f) for f in files)} {USER}@{HOST}:{WORKDIR}",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    logger.debug(f"stdout: {str(stdout, 'utf-8')}")
    logger.debug(f"stderr: {str(stderr, 'utf-8')}")


def launch_job(path: str):
    """Launch Slurm job on G100 with the converted user query

    Parameters
    ----------
    path : str
        path to the Slurm script containing the job info
    """

    cmd = f"cd {WORKDIR}; sbatch {path}"

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"ssh -i /home/centos/.ssh/luca-hpc {USER}@{HOST} '{cmd}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    logger.debug(f"stdout: {str(stdout, 'utf-8')}")
    logger.debug(f"stderr: {str(stderr, 'utf-8')}")
