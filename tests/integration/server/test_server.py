import pytest

#
# Testing the launcher module
#

import os
import subprocess
import json

from conftest import ROOT_DIR
from dlaas.tuilib.common import Config


def test_just_search(config_server: Config, setup_testfiles_HPC):
    """
    Search for two specific files
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )

    os.system(f"{ROOT_DIR}/dlaas/bin/dl_tui_server.py input.json")

    while True:
        # checking that JOB_DONE file has been made
        if os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DLAAS-TUI-TEST/JOB_DONE'") == 0:
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


def test_return_first(config_server: Config, setup_testfiles_HPC):
    """
    Search for two specific files and only return the first item
    """

    config = config_server
    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "script": "user_script.py",
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{ROOT_DIR}/dlaas/bin/dl_tui_server.py input.json")

    while True:
        # checking that JOB_DONE file has been made
        if os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/DLAAS-TUI-TEST/JOB_DONE'") == 0:
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


def test_invalid_script(setup_testfiles_HPC):
    """
    Try breaking the job
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST-invalid_job",
                "query": 'SELECT * FROM metadata WHERE id = "1" OR id = "2"',
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )

    stdout, stderr = subprocess.Popen(
        f"{ROOT_DIR}/dlaas/bin/dl_tui_server.py input.json",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert stderr[:35] == b"Traceback (most recent call last):\n", "job was launched, should not have worked"
