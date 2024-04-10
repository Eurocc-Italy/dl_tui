import pytest

#
# Testing the launcher module
#

import os
import subprocess
import json
from time import sleep
from zipfile import ZipFile

from conftest import ROOT_DIR
from dlaas.tuilib.common import Config


def check_status():
    """Checks that results have been uploaded to the data lake, and then download the results archive"""
    while True:
        # checking that results file has been uploaded
        stdout, stderr = subprocess.Popen(
            f"{ROOT_DIR}/dlaas/bin/dl_tui.py --browse --filter='job_id = DLAAS-TUI-TEST'",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).communicate()

        if f"results_DLAAS-TUI-TEST.zip" in stdout.decode("utf-8"):
            # download results archive
            sleep(5)  # wait for the download to be available
            subprocess.Popen(
                f"{ROOT_DIR}/dlaas/bin/dl_tui.py --download --key=results_DLAAS-TUI-TEST.zip", shell=True
            ).communicate()
            sleep(5)  # wait for the download to be completed
            break


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

    check_status()

    # checking zip content
    with ZipFile(f"results_DLAAS-TUI-TEST.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            "query_DLAAS-TUI-TEST.txt",
            "test1.txt",
            "test2.txt",
        ], "Results archive does not contain the expected files."


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
                "script_path": "user_script.py",
                "config_server": {"walltime": "00:10:00", "ntasks_per_node": 1},
            },
            f,
        )
    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    os.system(f"{ROOT_DIR}/dlaas/bin/dl_tui_server.py input.json")

    check_status()

    # checking zip content
    with ZipFile(f"results_DLAAS-TUI-TEST.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            "query_DLAAS-TUI-TEST.txt",
            "test1.txt",
            "user_script_DLAAS-TUI-TEST.py",
        ], "Results archive does not contain the expected files."


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
