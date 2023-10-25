"""
Functions and utilities to interface with HPC (G100) from the VM

!!! WIP !!!

Author: @lbabetto
"""

import os
import subprocess
from pymongo import MongoClient


# Temporary, will need to be replaced with chain user and relative home directory
# HPC
HPC_USER = "lbabetto"
HPC_HOST = "g100"
HPC_WORKDIR = f"/g100/home/userinternal/lbabetto/PROJECTS/1-DTaas/{os.path.basename(os.getcwd())}"

# MongoDB
VM_SUBMIT_DIR = os.getcwd()
DB_USER = "user"
DB_PASSWORD = "passwd"
DB_IP = "131.175.207.101"
DB_PORT = "27017"

MONGODB_URI = f"mongodb://{DB_USER}:{DB_PASSWORD}@{DB_IP}:{DB_PORT}/"
CLIENT = MongoClient(MONGODB_URI)


def launch_job(script, files):
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter"""

    CORE = os.path.dirname(__file__)
    cmd = f"cd {HPC_WORKDIR}; scp vm:{VM_SUBMIT_DIR}/{script} vm:{CORE}/slurm.sh .; sed -i 's/SCRIPT/{CORE}\/wrapper.py/' slurm.sh; sbatch slurm.sh"

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"ssh -i /home/centos/.ssh/luca-hpc {HPC_USER}@{HPC_HOST} '{cmd}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    print(f"stdout: {str(stdout, 'utf-8')}")
    print(f"stderr: {str(stderr, 'utf-8')}")
