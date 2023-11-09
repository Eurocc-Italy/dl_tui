import pytest

#
# Testing the launcher module
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

import os
import subprocess
from dtaas.tuilib.common import Config, UserInput, sanitize_string


def test_just_search():
    """
    Search for two specific files
    """

    config = Config("server")
    user_input = UserInput(
        {
            "ID": "DTAAS-TUI-TEST-just_search",
            "query": "SELECT * FROM metadata WHERE id = 554625 OR id = 222564",
            "script": r"def main(files_in):\n files_out=files_in.copy()\n return [files_out[0]]",
        }
    )

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_server "
    cmd += f'{{"ID": "{user_input.id}", "query": "{user_input.query}"}}'
    os.system(sanitize_string(version="client", string=cmd))

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

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_server "
    cmd += f'{{"ID": "{user_input.id}", "query": "{user_input.query}"}}'
    os.system(sanitize_string(version="client", string=cmd))

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

    cmd = f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_server "
    cmd += f'{{"ID": "{user_input.id}", "query": "{user_input.query}"}}'

    stdout, stderr = subprocess.Popen(
        sanitize_string(version="client", string=cmd),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).communicate()

    assert stdout == b""
    assert (
        stderr[-82:] == b"json.decoder.JSONDecodeError: Expecting ',' delimiter: line 1 column 83 (char 82)\n"
    ), "job was launched, should not have worked"
