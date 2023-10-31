import pytest
import json
import os

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


def run_query(query: str):
    """Runs a query using the dtaas_wrapper script and returns MongoDB query + filters

    Parameters
    ----------
    query : str
        SQL query

    Returns
    -------
    str
        MongoDB query (WHERE part)
    str
        MongoDB query (SELECT part)
    """
    with open("config.json", "w") as f:
        json.dump(config, f)
    os.system(f'python {TEST_DIR}/../../dtaas/wrapper.py --query """{query}"""')
    with open("logfile.log", "r") as f:
        for line in f:
            print(line)
            if "query filter" in line:
                mongo_filter = line.lstrip("MongoDB query filter: ").rstrip("\n")
            if "query fields" in line:
                mongo_fields = line.lstrip("MongoDB query fields: ").rstrip("\n")

    os.remove("logfile.log")
    os.remove("config.json")

    return mongo_filter, mongo_fields


#
# Testing various SQL queries to make sure they are correctly processed by the parser engine
#


# get all data
def test_get_everything():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM metadata""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{}"
        assert mongo_fields == "{}"


# try lowercase "select"
def test_case_lower_select():
    try:
        mongo_filter, mongo_fields = run_query("""select * FROM metadata""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{}"
        assert mongo_fields == "{}"


# try lowercase "from"
def test_case_lower_from():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * from metadata""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{}"
        assert mongo_fields == "{}"


# try lowercase "where"
def test_case_lower_where():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake where category = motorcycle""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'category': 'motorcycle'}"
        assert mongo_fields == "{}"


# try encasing argument in '' quotes
def test_quoted_argument_single():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake where category = 'motorcycle'""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'category': 'motorcycle'}"
        assert mongo_fields == "{}"


# try encasing argument in "" quotes
def test_quoted_argument_double():
    try:
        mongo_filter, mongo_fields = run_query('''SELECT * FROM datalake WHERE width > "600"''')
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'width': {'$gt': 600}}"
        assert mongo_fields == "{}"


# try encasing parameter in "" quotes
def test_quoted_parameter():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake where "category" = motorcycle""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'category': 'motorcycle'}"
        assert mongo_fields == "{}"


# try with multi-word argument
def test_multiword_argument():
    try:
        mongo_filter, mongo_fields = run_query(
            """SELECT * FROM datalake where caption = 'a boy wearing headphones using one computer in a long row of computers'"""
        )
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'caption': 'a boy wearing headphones using one computer in a long row of computers'}"
        assert mongo_fields == "{}"


# selecting data where category = motorcycle
def test_single_where_equal():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake WHERE category = motorcycle""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'category': 'motorcycle'}"
        assert mongo_fields == "{}"


# selecting data where width > 600
def test_single_where_gt():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake WHERE width > 600""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'width': {'$gt': 600}}"
        assert mongo_fields == "{}"


# selecting data where category = motorcycle and width > 600
def test_and():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake WHERE category = motorcycle AND width > 600""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'$and': [{'category': 'motorcycle'}, {'width': {'$gt': 600}}]}"
        assert mongo_fields == "{}"


# selecting data where category = motorcycle or category = bicycle
def test_or():
    try:
        mongo_filter, mongo_fields = run_query(
            """SELECT * FROM datalake WHERE category = motorcycle OR category = bicycle"""
        )
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'$or': [{'category': 'motorcycle'}, {'category': 'bicycle'}]}"
        assert mongo_fields == "{}"


# selecting data where category != motorcycle
def test_not():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT * FROM datalake WHERE NOT category = motorcycle""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'$nor': [{'category': 'motorcycle'}]}"
        assert mongo_fields == "{}"


# selecting width, height for all data
def test_select():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT width, height FROM datalake""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{}"
        assert mongo_fields == "{'width': 1, 'height': 1}"


# selecting width, height for all data where category = motorcycle
def test_select_where():
    try:
        mongo_filter, mongo_fields = run_query("""SELECT width, height FROM datalake WHERE category = motorcycle""")
    except:
        assert False, "Unexpected exception."
    else:
        assert mongo_filter == "{'category': 'motorcycle'}"
        assert mongo_fields == "{'width': 1, 'height': 1}"


# complex SQL query, to check if everything works
def test_complex_SQL():
    try:
        mongo_filter, mongo_fields = run_query(
            """SELECT a, b from User WHERE NOT ( last_name = 'Jacob' OR ( first_name != 'Chris' AND last_name != 'Lyon' ) ) AND NOT is_active = 1"""
        )
    except:
        assert False, "Unexpected exception."
    else:
        assert (
            mongo_filter
            == "{'$and': [{'$nor': [{'$or': [{'last_name': 'Jacob'}, {'$and': [{'first_name': {'$ne': 'Chris'}}, {'last_name': {'$ne': 'Lyon'}}]}]}]}, {'$nor': [{'is_active': 1}]}]}"
        )
        assert mongo_fields == "{'a': 1, 'b': 1}"


# complex SQL query, to check if everything works, with as bad of a syntax as possible
# TODO: implement in sqlparse parsing of field names within quotes?
def test_complex_SQL_mangled():
    try:
        mongo_filter, mongo_fields = run_query(
            """select a,b from User where not (last_name=Jacob or (first_name!= 'Chris' and last_name!='Lyon'))and not is_active = 1"""
        )
    except:
        assert False, "Unexpected exception."
    else:
        assert (
            mongo_filter
            == "{'$and': [{'$nor': [{'$or': [{'last_name': 'Jacob'}, {'$and': [{'first_name': {'$ne': 'Chris'}}, {'last_name': {'$ne': 'Lyon'}}]}]}]}, {'$nor': [{'is_active': 1}]}]}"
        )
        assert mongo_fields == "{'a': 1, 'b': 1}"
