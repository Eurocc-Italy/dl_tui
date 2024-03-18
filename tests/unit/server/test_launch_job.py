import pytest

from dlaas.tuilib.common import Config

#
# Testing the launch_job function in module server.py
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
from dlaas.tuilib.server import create_remote_directory, copy_json_input, copy_user_script, launch_job


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

    stdout, stderr, slurm_job_id = launch_job(json_path="input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/JOB_DONE'"
            )
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        == 0
    ), "Results file not found."

    # checking that the upload script is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/upload_results_DLAAS-TUI-TEST.py'"
        )
        == 0
    ), "Upload script not found."

    # checking zip content
    assert (
        "test1.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        .read()
        .split()
        and "test2.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        .read()
        .split()
    ), "Missing output file."

    # checking that slurm output is empty
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DLAAS-TUI-TEST/slurm*'"
        ).read()
        == ""
    ), "Slurm output file is not empty."


def test_full_path(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Search for two specific files using full path for input.json
    """

    with open(f"{os.getcwd()}/input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "config_server": config_server.__dict__,
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    create_remote_directory(json_path=f"{os.getcwd()}/input.json")
    copy_json_input(json_path=f"{os.getcwd()}/input.json")
    copy_user_script(json_path=f"{os.getcwd()}/input.json")

    stdout, stderr, slurm_job_id = launch_job(json_path=f"{os.getcwd()}/input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/JOB_DONE'"
            )
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        == 0
    ), "Results file not found."

    # checking that the upload script is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/upload_results_DLAAS-TUI-TEST.py'"
        )
        == 0
    ), "Upload script not found."

    # checking zip content
    assert (
        "test1.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        .read()
        .split()
        and "test2.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        .read()
        .split()
    ), "Missing output file."

    # checking that slurm output is empty
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DLAAS-TUI-TEST/slurm*'"
        ).read()
        == ""
    ), "Slurm output file is not empty."


def test_return_first(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "script": "user_script.py",
                "config_server": config_server.__dict__,
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")

    stdout, stderr, slurm_job_id = launch_job(json_path="input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/JOB_DONE'"
            )
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        == 0
    ), "Results file not found."

    # checking that the upload script is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/upload_results_DLAAS-TUI-TEST.py'"
        )
        == 0
    ), "Upload script not found."

    # checking zip content
    assert (
        "test1.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        .read()
        .split()
    ), "Missing output file."

    # checking that slurm output is empty
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DLAAS-TUI-TEST/slurm*'"
        ).read()
        == ""
    ), "Slurm output file is not empty."


def test_invalid_script(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Try breaking the job
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "blablabla",
                "config_server": config_server.__dict__,
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )
    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")

    stdout, stderr, slurm_job_id = launch_job(json_path="input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/JOB_DONE'"
            )
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/results_DLAAS-TUI-TEST.zip'"
        )
        == 512
    ), "Results file found, should not have worked."

    # checking that the upload script is NOT present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DLAAS-TUI-TEST/upload_results_DLAAS-TUI-TEST.py'"
        )
        == 512
    ), "Upload script found, should not have been present."

    # checking that slurm output contains an error
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DLAAS-TUI-TEST/slurm*'"
        ).read()[:35]
        == "Traceback (most recent call last):\n"
    ), "Unexpected output in Slurm file."
