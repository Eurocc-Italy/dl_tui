"""
Functions and utilities to interface with HPC (G100) from the VM

!!! WIP !!!

Author: @lbabetto
"""

import os
import subprocess


# Temporary, will need to be replaced with chain user and relative home directory
USER = "lbabetto"
HOST = "g100"
WORKDIR = f"/g100/home/userinternal/lbabetto/PROJECTS/1-DTaas/{os.path.basename(os.getcwd())}"
SUBMIT_DIR = os.getcwd()


def launch_job(script, files):
    """Launch Slurm job on G100 with the user script on the files returned by the TUI filter"""

    with open("FILES", "w") as f:
        for file in files:
            f.write(f"{file}\n")

    cmd = f"cd {WORKDIR}; scp vm:{SUBMIT_DIR}/{script} vm:{SUBMIT_DIR}/FILES vm:{os.path.dirname(__file__)}/slurm.sh .; sed -i 's/SCRIPT/{script}/' slurm.sh; sbatch slurm.sh"

    stdout, stderr = subprocess.Popen(
        # key currently necessary, will be removed when we switch to chain user
        f"ssh -i /home/centos/.ssh/luca-hpc {USER}@{HOST} '{cmd}'",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    print(f"stdout: {str(stdout, 'utf-8')}")
    print(f"stderr: {str(stderr, 'utf-8')}")
