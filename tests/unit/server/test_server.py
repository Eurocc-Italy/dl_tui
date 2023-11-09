import pytest

#
# Testing the launcher module
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
from dtaas.tuilib.common import Config, UserInput
from dtaas.tuilib.server import launch_job


def test_just_search():
    """
    Search for two specific files
    """

    config = Config("server")
    user_input = UserInput(
        {
            "ID": "DTAAS-TUI-TEST-just_search",
            "query": "SELECT * FROM metadata WHERE id = 554625 OR id = 222564",
        }
    )
    stdout, stderr = launch_job(config=config, user_input=user_input)

    assert stdout[:19] == "Submitted batch job"

    while True:
        # checking that JOB_DONE file has been made
        if os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/{user_input.id}/JOB_DONE'") == 0:
            break

    # checking that results file is present
    assert (
        os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/{user_input.id}/results.zip'") == 0
    ), "Results file not found"
    # checking that slurm output is empty
    assert (
        os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/{user_input.id}/slurm*'").read() == " \n"
    ), "Slurm output file is not empty"


def test_return_first():
    """
    Search for two specific files and only return the first item
    """

    config = Config("server")
    user_input = UserInput(
        {
            "ID": "DTAAS-TUI-TEST-return_first",
            "query": "SELECT * FROM metadata WHERE id = 554625 OR id = 222564",
            "script": r"def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]",
        }
    )
    stdout, stderr = launch_job(config=config, user_input=user_input)

    assert stdout[:19] == "Submitted batch job"

    while True:
        # checking that JOB_DONE file has been made
        if os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/{user_input.id}/JOB_DONE'") == 0:
            # checking that results file is present
            assert (
                os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/{user_input.id}/results.zip'")
                == 0
            ), "Results file not found"
            # checking zip content
            assert (
                "COCO_val2014_000000554625.jpg"
                in os.popen(
                    f"ssh -i {config.ssh_key} {config.user}@{config.host} 'unzip -l ~/{user_input.id}/results.zip'"
                )
                .read()
                .split()
            ), "Missing output file"
            # checking that slurm output is empty
            assert (
                os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/{user_input.id}/slurm*'").read()
                == " \n"
            ), "Slurm output file is not empty"
            break


def test_invalid_script():
    """
    Try breaking the job
    """

    config = Config("server")
    user_input = UserInput(
        {
            "ID": "DTAAS-TUI-TEST-invalid_job",
            "query": 'SELECT * FROM metadata WHERE id = "554625" OR id = 222564',
        }
    )
    stdout, stderr = launch_job(config=config, user_input=user_input)

    assert stdout[:19] == "Submitted batch job"

    while True:
        # checking that JOB_DONE file has been made
        if os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/{user_input.id}/JOB_DONE'") == 0:
            # checking that results file is present
            assert (
                os.system(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'ls ~/{user_input.id}/results.zip'")
                == 512
            ), "Results file found, should not have worked"

            # checking that slurm output contains an error
            assert (
                os.popen(f"ssh -i {config.ssh_key} {config.user}@{config.host} 'cat ~/{user_input.id}/slurm*'").read()[
                    -82:
                ]
                == "json.decoder.JSONDecodeError: Expecting ',' delimiter: line 1 column 83 (char 82)\n"
            ), "Slurm output file is not empty"
            break
