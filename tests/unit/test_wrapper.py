import pytest
import json
import os

#
# using sample data from https://github.com/neelabalan/mongodb-sample-dataset (sample_airbnb dataset)
#

config = {
    "MONGO": {
        "user": "user",
        "password": "passwd",
        "ip": "localhost",
        "port": "27017",
        "database": "sample_airbnb",
        "collection": "listingsAndReviews",
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
    with open("QUERY", "w") as f:
        f.write(query)
    with open("script.py", "w") as f:
        f.write(script)
    os.system(f"python {TEST_DIR}/../../dtaas/dtaas_wrapper")
    with open("logfile.log", "r") as f:
        files_in = []
        files_out = []
        for line in f:
            print(line, end="")
            if "Query results:" in line:
                files_in = json.loads(line.lstrip("Query results:").rstrip("\n"))
            if "Processed results:" in line:
                files_out = json.loads(line.lstrip("Processed results: ").rstrip("\n"))

    os.remove("logfile.log")
    os.remove("QUERY")
    os.remove("config.json")
    os.remove("script.py")

    return files_in, files_out


#
# Testing
#


# test
def test_1():
    try:
        files_in, files_out = run_query(
            """SELECT number_of_reviews, bedrooms FROM listingsAndReviews WHERE name = 'Ribeira Charming Duplex'""",
            """def main(files_in):\n    return []""",
        )
    except:
        assert False, "Unexpected exception."
    else:
        assert files_in == [{"_id": "10006546", "bedrooms": 3, "number_of_reviews": 51}]
        assert files_out == []
