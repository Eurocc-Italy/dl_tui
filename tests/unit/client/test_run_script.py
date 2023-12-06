import pytest

#
# Testing run_script function in client.py library
#

from dtaas.tuilib.client import run_script
import os
import shutil
from sh import pushd


@pytest.fixture(scope="function")
def create_tmpdir():
    tmpdir = "run_script_test"
    os.makedirs(tmpdir)
    yield tmpdir
    shutil.rmtree(tmpdir)


def test_run_script(generate_test_files, create_tmpdir):
    """
    Search for two specific files and return them in reverse order
    """

    script = "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"

    files_in = generate_test_files

    with pushd(create_tmpdir):
        files_out = run_script(script=script, files_in=files_in)

    assert files_out == [
        f"{os.getcwd()}/run_script_test/TESTFILE_2.txt",
        f"{os.getcwd()}/run_script_test/TESTFILE_1.txt",
    ], "Output file list not matching"


def test_missing_main(create_tmpdir):
    """
    Test that if no `main` function is present, the program crashes
    """
    with pytest.raises(AttributeError):
        script = "answer = 42"
        files_in = []
        with pushd(create_tmpdir):
            run_script(script=script, files_in=files_in)


def test_main_wrong_return(create_tmpdir):
    """
    Test that if `main` function does not return a list, the program crashes
    """
    with pytest.raises(TypeError):
        script = "def main(files_in):\n return 42"
        files_in = []
        with pushd(create_tmpdir):
            run_script(script=script, files_in=files_in)
