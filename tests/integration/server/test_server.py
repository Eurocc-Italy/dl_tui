import pytest

#
# Testing the launcher module
#

import os
import subprocess
import json

from conftest import ROOT_DIR


def test_just_search(config_server):
    """
    Search for two specific files
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-just_search",
                "sql_query": "SELECT * FROM metadata WHERE id = 1 OR id = 2",
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )

    os.system(f"{ROOT_DIR}/dtaas/bin/dtaas_tui_server.py input.json")

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DTAAS-TUI-TEST-just_search/JOB_DONE'")
            == 0
        ):
            break

    # checking that results file is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-just_search/results_DTAAS-TUI-TEST-just_search.zip'"
        )
        == 0
    ), "Results file not found."

    # checking that the upload script is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-just_search/upload_results_DTAAS-TUI-TEST-just_search.py'"
        )
        == 0
    ), "Upload script not found."

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
    ), "Missing output file."

    # checking that slurm output is empty
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DTAAS-TUI-TEST-just_search/slurm*'"
        ).read()
        == ""
    ), "Slurm output file is not empty."


def test_return_first(config_server):
    """
    Search for two specific files and only return the first item
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-return_first",
                "sql_query": "SELECT * FROM metadata WHERE id = 1 OR id = 2",
                "script": "user_script.py",
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{ROOT_DIR}/dtaas/bin/dtaas_tui_server.py input.json")

    while True:
        # checking that JOB_DONE file has been made
        if (
            os.system(
                f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DTAAS-TUI-TEST-return_first/JOB_DONE'"
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
    ), "Results file not found."

    # checking that the upload script is present
    assert (
        os.system(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'ls ~/DTAAS-TUI-TEST-return_first/upload_results_DTAAS-TUI-TEST-return_first.py'"
        )
        == 0
    ), "Upload script not found."

    # checking zip content
    assert (
        "test1.txt"
        in os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'unzip -l ~/DTAAS-TUI-TEST-return_first/results_DTAAS-TUI-TEST-return_first.zip'"
        )
        .read()
        .split()
    ), "Missing output file."

    # checking that slurm output is empty
    assert (
        os.popen(
            f"ssh -i {config_server.ssh_key} {config_server.user}@{config_server.host} 'cat ~/DTAAS-TUI-TEST-return_first/slurm*'"
        ).read()
        == ""
    ), "Slurm output file is not empty."


def test_invalid_script():
    """
    Try breaking the job
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DTAAS-TUI-TEST-invalid_job",
                "query": 'SELECT * FROM metadata WHERE id = "554625" OR id = 222564',
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dtaas/bin/dtaas_tui_server.py input.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[:35] == b"Traceback (most recent call last):\n", "job was launched, should not have worked"
