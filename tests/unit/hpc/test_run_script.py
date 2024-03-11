import pytest

#
# Testing run_script function in hpc.py library
#

from dlaas.tuilib.hpc import run_script
import os
import shutil
from sh import pushd

from conftest import ROOT_DIR


@pytest.fixture(scope="function")
def create_tmpdir():
    tmpdir = "run_script_test"
    os.makedirs(tmpdir)
    yield tmpdir
    shutil.rmtree(tmpdir)


def test_run_script(create_tmpdir):
    """
    Search for two specific files and return them in reverse order
    """

    script = "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"

    files_in = [
        f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
        f"{ROOT_DIR}/tests/utils/sample_files/test2.txt",
    ]

    with pushd(create_tmpdir):
        files_out = run_script(script=script, files_in=files_in)

    assert files_out == [
        f"{ROOT_DIR}/tests/utils/sample_files/test2.txt",
        f"{ROOT_DIR}/tests/utils/sample_files/test1.txt",
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
