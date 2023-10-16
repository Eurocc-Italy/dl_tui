import os
import subprocess

user = "lbabetto"
host = "login.g100.cineca.it"

workdir = "/g100/home/userinternal/lbabetto/PROJECTS/1-DTaas/2-remote-job-launch"

## prepare files
stdout, stderr = subprocess.Popen(
    f"scp -i /home/centos/.ssh/luca-hpc ./* {user}@{host}:{workdir}",
    shell=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
).communicate()

print(f"stdout: {str(stdout, 'utf-8')}")
print(f"stderr: {str(stderr, 'utf-8')}")

# run script
cmd = f"cd {workdir}; sbatch job-launch.slurm"

stdout, stderr = subprocess.Popen(
    f"ssh -i /home/centos/.ssh/luca-hpc {user}@{host} '{cmd}'",
    shell=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
).communicate()

print(f"stdout: {str(stdout, 'utf-8')}")
print(f"stderr: {str(stderr, 'utf-8')}")
