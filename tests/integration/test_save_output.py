import pytest

#
# Testing save_output function in wrapper.py
#
# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

from dtaas.wrapper import convert_SQL_to_mongo, retrieve_files, run_script


def test_search_specific_files():
    """
    Search for two specific files and return them in reverse order
    """
    mongo_filter, mongo_fields = convert_SQL_to_mongo("""SELECT * FROM metadata WHERE id = 554625 OR id = 222564""")
    files_in = retrieve_files(query_filters=mongo_filter, query_fields=mongo_fields)
    files_out = run_script(
        script="""def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out""",
        files_in=files_in,
    )
    assert files_in == [
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
    ], "Input file list not matching"
    assert files_out == [
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
        "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
    ], "Output file list not matching"
