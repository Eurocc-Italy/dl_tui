import pytest

#
# Testing the launch_job function in module server.py
#
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
from dtaas.tuilib.server import create_remote_directory, copy_json_input, copy_user_script, launch_job


def test_just_search(config_server, config_client):
    """
    Search for two specific files
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-just_search",
                "sql_query": "SELECT * FROM metadata WHERE id = 1 OR id = 2",
                "config_server": config_server.__dict__,
                "config_client": config_client.__dict__,
            },
            f,
        )

    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")

    stdout, stderr = launch_job(json_path="input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-just_search/JOB_DONE'"
            )
            == 0
        ):
            break

        # checking that results file is present
        assert (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-just_search/results_DTAAS-TUI-TEST-just_search.zip'"
            )
            == 0
        ), "Results file not found"
        # checking zip content
        assert (
            "test1.txt"
            in os.popen(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DTAAS-TUI-TEST-just_search/results_DTAAS-TUI-TEST-just_search.zip'"
            )
            .read()
            .split()
            and "test2.txt"
            in os.popen(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DTAAS-TUI-TEST-just_search/results_DTAAS-TUI-TEST-just_search.zip'"
            )
            .read()
            .split()
        ), "Missing output file"
        # checking that slurm output is empty
        assert (
            os.popen(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DTAAS-TUI-TEST-just_search/slurm*'"
            ).read()
            == " \n"
        ), "Slurm output file is not empty"


def test_return_first(config_server, config_client):
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-return_first",
                "sql_query": "SELECT * FROM metadata WHERE id = 1 OR id = 2",
                "script": "user_script.py",
                "config_server": config_server.__dict__,
                "config_client": config_client.__dict__,
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")

    stdout, stderr = launch_job(json_path="input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-return_first/JOB_DONE'"
            )
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-return_first/results_DTAAS-TUI-TEST-return_first.zip'"
        )
        == 0
    ), "Results file not found"
    # checking zip content
    assert (
        "test1.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DTAAS-TUI-TEST-return_first/results_DTAAS-TUI-TEST-return_first.zip'"
        )
        .read()
        .split()
    ), "Missing output file"
    # checking that slurm output is empty
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DTAAS-TUI-TEST-return_first/slurm*'"
        ).read()
        == " \n"
    ), "Slurm output file is not empty"


def test_invalid_script(config_server, config_client):
    """
    Try breaking the job
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-invalid_job",
                "sql_query": "blablabla",
                "config_server": config_server.__dict__,
                "config_client": config_client.__dict__,
            },
            f,
        )
    create_remote_directory(json_path="input.json")
    copy_json_input(json_path="input.json")
    copy_user_script(json_path="input.json")

    stdout, stderr = launch_job(json_path="input.json")

    assert stdout[:19] == "Submitted batch job"
    assert stderr == ""

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-invalid_job/JOB_DONE'"
            )
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-invalid_job/results_DTAAS-TUI-TEST-invalid_job.zip'"
        )
        == 512
    ), "Results file found, should not have worked"

    # checking that slurm output contains an error
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DTAAS-TUI-TEST-invalid_job/slurm*'"
        ).read()[:35]
        == " \nTraceback (most recent call last)"
    ), "Unexpected output in Slurm file."
