import pytest

#
# Testing convert_SQL_to_mongo function in client.py library
#

from dtaas.tuilib.client import convert_SQL_to_mongo


def test_get_everything():
    """
    Get all data
    """
    query = "SELECT * FROM metadata"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {}
    assert mongo_fields == {}


def test_case_lower_select():
    """
    Try lowercase select
    """
    query = "select * FROM metadata"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {}
    assert mongo_fields == {}


def test_case_lower_from():
    """
    Try lowercase 'from'
    """
    query = "SELECT * from metadata"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {}
    assert mongo_fields == {}


def test_case_lower_where():
    """
    Try lowercase 'where'
    """
    query = "SELECT * FROM datalake where category = motorcycle"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"category": "motorcycle"}
    assert mongo_fields == {}


def test_quoted_argument_single():
    """
    Try encasing argument in single quotes
    """
    query = "SELECT * FROM datalake where category = 'motorcycle'"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"category": "motorcycle"}
    assert mongo_fields == {}


def test_quoted_argument_double():
    """
    Try encasing argument in double quotes
    """
    query = 'SELECT * FROM datalake WHERE width > "600"'
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"width": {"$gt": "600"}}
    assert mongo_fields == {}


# TODO: currently not working, parser does not accept quoted parameters...
@pytest.mark.xfail
def test_quoted_parameter():
    """
    Try encasing parameter in double quotes
    """
    query = 'SELECT * FROM datalake where "category" = motorcycle'
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"category": "motorcycle"}
    assert mongo_fields == {}


def test_multiword_argument():
    """
    Try with multi-word argument
    """
    query = "SELECT * FROM datalake where caption = 'a boy wearing headphones using one computer in a long row of computers'"

    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"caption": "a boy wearing headphones using one computer in a long row of computers"}
    assert mongo_fields == {}


def test_single_where_equal():
    """
    Selecting data where category = motorcycle
    """
    query = "SELECT * FROM datalake WHERE category = motorcycle"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"category": "motorcycle"}
    assert mongo_fields == {}


def test_single_where_gt():
    """
    Selecting data where width > 600
    """
    query = "SELECT * FROM datalake WHERE width > 600"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"width": {"$gt": 600}}
    assert mongo_fields == {}


def test_and():
    """
    Selecting data where category = motorcycle and width > 600
    """
    query = "SELECT * FROM datalake WHERE category = motorcycle AND width > 600"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"$and": [{"category": "motorcycle"}, {"width": {"$gt": 600}}]}
    assert mongo_fields == {}


def test_or():
    """
    Selecting data where category = motorcycle or category = bicycle
    """
    query = "SELECT * FROM datalake WHERE category = motorcycle OR category = bicycle"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"$or": [{"category": "motorcycle"}, {"category": "bicycle"}]}
    assert mongo_fields == {}


def test_not():
    """
    Selecting data where category != motorcycle
    """
    query = "SELECT * FROM datalake WHERE NOT category = motorcycle"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"$nor": [{"category": "motorcycle"}]}
    assert mongo_fields == {}


def test_select():
    """
    Selecting width, height for all data
    """
    query = "SELECT width, height FROM datalake"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {}
    assert mongo_fields == {"width": 1, "height": 1}


def test_select_where():
    """
    Selecting width, height for all data where category = motorcycle
    """
    query = "SELECT width, height FROM datalake WHERE category = motorcycle"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"category": "motorcycle"}
    assert mongo_fields == {"width": 1, "height": 1}


def test_newlines():
    """
    Testing SQL statement with newlines
    """
    query = "SELECT *\nFROM datalake\nWHERE category = motorcycle"
    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {"category": "motorcycle"}
    assert mongo_fields == {}


def test_complex_SQL():
    """
    Complex SQL query, to check if everything works
    """
    query = "SELECT a, b from User WHERE NOT ( last_name = 'Jacob' OR ( first_name != 'Chris' AND last_name != 'Lyon' ) ) AND NOT is_active = 1"

    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {
        "$and": [
            {
                "$nor": [
                    {
                        "$or": [
                            {"last_name": "Jacob"},
                            {
                                "$and": [
                                    {"first_name": {"$ne": "Chris"}},
                                    {"last_name": {"$ne": "Lyon"}},
                                ]
                            },
                        ]
                    }
                ]
            },
            {"$nor": [{"is_active": 1}]},
        ]
    }
    assert mongo_fields == {"a": 1, "b": 1}


# TODO: implement in sqlparse parsing of field names within quotes?
def test_complex_SQL_mangled():
    """
    Complex SQL query, to check if everything works, with as bad of a syntax as possible
    """
    query = "select a,b from User where not (last_name=Jacob or (first_name!= 'Chris' and last_name!='Lyon'))and not is_active = 1"

    mongo_filter, mongo_fields = convert_SQL_to_mongo(query)
    assert mongo_filter == {
        "$and": [
            {
                "$nor": [
                    {
                        "$or": [
                            {"last_name": "Jacob"},
                            {
                                "$and": [
                                    {"first_name": {"$ne": "Chris"}},
                                    {"last_name": {"$ne": "Lyon"}},
                                ]
                            },
                        ]
                    }
                ]
            },
            {"$nor": [{"is_active": 1}]},
        ]
    }
    assert mongo_fields == {"a": 1, "b": 1}
