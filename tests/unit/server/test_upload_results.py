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
from time import sleep
from zipfile import ZipFile

from dlaas.tuilib.api import browse, download

# loading IP and token from defaults
ip = Config("hpc").ip
with open(f"{os.environ['HOME']}/.config/dlaas/api-token.txt", "r") as f:
    token = f.read()


def check_status():
    """Checks that results have been uploaded to the data lake, and then download the results archive

    Parameters
    ----------
    job_id : str
        job identifier for file downloads
    """
    while True:
        sleep(1)
        # checking that results file has been uploaded
        response = browse(ip=ip, token=token, filter="job_id = DLAAS-TUI-TEST").text

        if "results_DLAAS-TUI-TEST.zip" in response:
            # download results archive
            sleep(5)  # wait for the download to be available
            download(ip=ip, token=token, file="results_DLAAS-TUI-TEST.zip")
            sleep(5)  # wait for the download to be completed
            break


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

    assert stdout == f"Submitted batch job {slurm_job_id+1}\n"
    assert stderr == ""

    check_status()

    # checking results archive
    assert os.path.exists(f"results_DLAAS-TUI-TEST.zip"), "Zipped archive was not created."

    with ZipFile(f"results_DLAAS-TUI-TEST.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            "query_DLAAS-TUI-TEST.txt",
            "test1.txt",
            "test2.txt",
        ], "Results archive does not contain the expected files."


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
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    create_remote_directory(json_path=f"{os.getcwd()}/input.json")
    copy_json_input(json_path=f"{os.getcwd()}/input.json")
    copy_user_script(json_path=f"{os.getcwd()}/input.json")
    _, _, slurm_job_id = launch_job(json_path=f"{os.getcwd()}/input.json")

    stdout, stderr = upload_results(json_path=f"{os.getcwd()}/input.json", slurm_job_id=slurm_job_id)

    assert stdout == f"Submitted batch job {slurm_job_id+1}\n"
    assert stderr == ""

    check_status()

    # checking results archive
    assert os.path.exists(f"results_DLAAS-TUI-TEST.zip"), "Zipped archive was not created."

    with ZipFile(f"results_DLAAS-TUI-TEST.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            "query_DLAAS-TUI-TEST.txt",
            "test1.txt",
            "test2.txt",
        ], "Results archive does not contain the expected files."


def test_return_first(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Search for two specific files and only return the first item
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "SELECT * FROM metadata WHERE id = '1' OR id = '2'",
                "script_path": "user_script.py",
                "config_server": config_server.__dict__,
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]")

    create_remote_directory(json_path=f"{os.getcwd()}/input.json")
    copy_json_input(json_path=f"{os.getcwd()}/input.json")
    copy_user_script(json_path=f"{os.getcwd()}/input.json")
    _, _, slurm_job_id = launch_job(json_path=f"{os.getcwd()}/input.json")

    stdout, stderr = upload_results(json_path=f"{os.getcwd()}/input.json", slurm_job_id=slurm_job_id)

    assert stdout == f"Submitted batch job {slurm_job_id+1}\n"
    assert stderr == ""

    check_status()

    # checking results archive
    assert os.path.exists(f"results_DLAAS-TUI-TEST.zip"), "Zipped archive was not created."

    with ZipFile(f"results_DLAAS-TUI-TEST.zip", "r") as archive:
        archive = archive.namelist()
        archive.sort()
        assert archive == [
            "query_DLAAS-TUI-TEST.txt",
            f"slurm-{slurm_job_id}.out",
            f"slurm-{slurm_job_id+1}.out",
            "test1.txt",
            "user_script_DLAAS-TUI-TEST.py",
        ], "Results archive does not contain the expected files."


def test_job_not_launched(config_server: Config, config_hpc: Config, setup_testfiles_HPC):
    """
    Make sure exception is raised if job is not actually launched
    """

    config_server_broken = config_server.__dict__
    config_server_broken["host"] = "wrong_host"

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "DLAAS-TUI-TEST",
                "sql_query": "blablabla",
                "config_server": config_server_broken,
                "config_hpc": config_hpc.__dict__,
            },
            f,
        )

    with pytest.raises(RuntimeError):
        stdout, stderr = upload_results(json_path="input.json", slurm_job_id=123)
