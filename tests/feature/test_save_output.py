import pytest

#
# Testing save_output function in wrapper.py
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from dtaas.wrapper import convert_SQL_to_mongo, retrieve_files, run_script, save_output
import os
from zipfile import ZipFile


def test_save_query_results(test_collection, generate_script):
    """
    Search for two specific files and save them in a zipped archive.
    """
    query = "SELECT * FROM metadata WHERE id = 554625 OR id = 222564"
    script_content = "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"

    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    files_in = retrieve_files(collection=test_collection, query_filters=mongo_filter, query_fields=mongo_fields)
    files_out = run_script(
        script_file=generate_script(script_content),
        files_in=files_in,
    )
    save_output(files_out=files_out)

    assert os.path.exists("results.zip"), "Zipped archive was not created."

    with ZipFile("results.zip", "r") as archive:
        assert archive.namelist() == ["COCO_val2014_000000222564.jpg", "COCO_val2014_000000554625.jpg"]

    os.remove("results.zip")
