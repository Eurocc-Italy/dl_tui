import pytest

#
# Testing run_script function in client.py library
#

from dtaas.lib.client import run_script


def test_run_script(generate_test_files):
    """
    Search for two specific files and return them in reverse order
    """

    script = "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"

    files_in = generate_test_files

    files_out = run_script(script=script, files_in=files_in)

    assert files_out == [
        "TESTFILE_2.txt",
        "TESTFILE_1.txt",
    ], "Output file list not matching"


def test_missing_main():
    """
    Test that if no `main` function is present, the program crashes
    """
    with pytest.raises(AttributeError):
        script = "answer = 42"
        files_in = []
        run_script(script=script, files_in=files_in)


def test_main_wrong_return():
    """
    Test that if `main` function does not return a list, the program crashes
    """
    with pytest.raises(TypeError):
        script = "def main(files_in):\n return 42"
        files_in = []
        run_script(script=script, files_in=files_in)
