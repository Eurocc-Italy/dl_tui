import pytest

#
# Testing save_output function in client.py library
#

from dtaas.lib.client import save_output
import os
from zipfile import ZipFile


def test_save_output(generate_test_files):
    """
    Test that the function actually generates a zip file.
    """

    save_output(generate_test_files)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        filelist = archive.namelist()
        filelist.sort()  # For some reason, the zip contains the files in reverse order...
        assert filelist == ["TESTFILE_1.txt", "TESTFILE_2.txt"]

    os.remove("results.zip")
