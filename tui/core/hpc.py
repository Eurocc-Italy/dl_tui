"""
Functions and utilities to interface with HPC (G100) from the VM

!!! WIP !!!

Author: @lbabetto
"""

import os
import subprocess

import logging

logger = logging.getLogger(__name__)

# Temporary, will need to be replaced with chain user and relative home directory
USER = "lbabetto"
HOST = "login.g100.cineca.it"
WORKDIR = "/g100/home/userinternal/lbabetto/PROJECTS/1-DTaas/3-test"
SUBMIT_DIR = "/home/centos/TESTS/1-filter"


def launch_job():
    """Launch Slurm job on G100 with the user script"""

    cmd = f"cd {WORKDIR}; scp vm:{SUBMIT_DIR}/main.py vm:{os.path.dirname(__file__)}/slurm.sh .; sbatch slurm.sh"

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"ssh -i /home/centos/.ssh/luca-hpc {USER}@{HOST} '{cmd}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    logger.debug(f"stdout: {str(stdout, 'utf-8')}")
    logger.debug(f"stderr: {str(stderr, 'utf-8')}")
