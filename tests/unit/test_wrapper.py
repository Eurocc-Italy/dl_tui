import pytest
import json
import os

# TODO: make a mock test file and test database so the tests do not rely on any previously prepared database

config = {
    "MONGO": {
        "user": "user",
        "password": "passwd",
        "ip": "localhost",
        "port": "27017",
        "database": "datalake",
        "collection": "metadata",
    },
    "LOGGING": {
        "logfile": "logfile.log",
        "format": "%(message)s",
        "level": "DEBUG",
    },
}

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


def run_query(query: str, script: str):
    """Runs a query using the dtaas_wrapper script and returns MongoDB query + filters

    Parameters
    ----------
    query : str
        SQL query
    script : str
        Python script containing the analysis to be ran on the query results
    Returns
    -------
    str
        MongoDB query (WHERE part)
    str
        MongoDB query (SELECT part)
    """
    with open("config.json", "w") as f:
        json.dump(config, f)

    print(f'python {TEST_DIR}/../../dtaas/wrapper.py --query """{query}""" --script """{script}"""')

    os.system(f"python {TEST_DIR}/../../dtaas/wrapper.py --query {query} --script {script}")

    with open("logfile.log", "r") as f:
        files_in = []
        files_out = []
        for line in f:
            print(line, end="")
            if "Query results:" in line:
                files_in = line.lstrip("Query results: [").rstrip("]\n").replace("'", "").split(", ")
            if "Processed results:" in line:
                files_out = line.lstrip("Processed results: [").rstrip("]\n").replace("'", "").split(", ")

    os.remove("logfile.log")
    os.remove("QUERY")
    os.remove("config.json")
    os.remove("script.py")

    return files_in, files_out


#
# Testing wrapper functionality, giving SQL queries and running Python processing scripts on the results.
#


# searching for two specific files and returning them in reverse order
def test_search_and_return():
    try:
        files_in, files_out = run_query(
            """SELECT * FROM metadata WHERE id = 554625 OR id = 222564""",
            """def main(files_in):\n    files_out = files_in\n    files_out.reverse()\n    return files_out""",
        )
    except:
        assert False, "Unexpected exception."
    else:
        assert files_in == [
            "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
            "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
        ]
        assert files_out == [
            "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000222564.jpg",
            "/home/lbabetto/PROJECTS/DTaaS/data/COCO/COCO_val2014_000000554625.jpg",
        ]
