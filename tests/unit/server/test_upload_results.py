import pytest

from dlaas.tuilib.common import Config

#
# Testing the upload_results function in module server.py
#
# NOTE: remember to delete the entries from the S3 bucket and MongoDB after testing!
"""
NOTE: for these tests to work, the following requirements must be met:

    1. The device must have access to a HPC infrastructure running Slurm, please set up the config_server fixture
    in conftest.py according to your specific setup.
    2. The HPC cluster must have access to a machine running a MongoDB server containing the datalake files metadata,
    which must include:
        - "id": identifier used in these tests for SQL search
        - "s3_key": S3 object key identifying the file on the S3 bucket
        - "path": POSIX path locating the file in the parallel filesystem on HPC (which must actually exist)
"""

import os
import json
from dlaas.tuilib.server import create_remote_directory, copy_json_input, copy_user_script, launch_job, upload_results


def test_just_search(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "config_server": config_server.__dict__,
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")
    _, _, slurm_job_id = launch_job(json_path="input.json")

    stdout, stderr = upload_results(json_path="input.json", slurm_job_id=slurm_job_id)

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that RESULTS_UPLOADED file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/RESULTS_UPLOADED'"
            )
            == 0
        ):
            break

    assert True


def test_full_path(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Search for two specific files
    """

    with open(f"{os.getcwd()}/input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "config_server": config_server.__dict__,
                "config": config_hpc.__dict__,
            },
            f,
        )

    create_remote_directory(json_path=f"{os.getcwd()}/input.json")
    copy_json_input(json_path=f"{os.getcwd()}/input.json")
    copy_user_script(json_path=f"{os.getcwd()}/input.json")
    _, _, slurm_job_id = launch_job(json_path=f"{os.getcwd()}/input.json")

    stdout, stderr = upload_results(json_path=f"{os.getcwd()}/input.json", slurm_job_id=slurm_job_id)

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that RESULTS_UPLOADED file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/RESULTS_UPLOADED'"
            )
            == 0
        ):
            break

    assert True
