#!/usr/bin/env python
#
# Command line utility to run SQL queries against a mongodb database
# or to show what the SQL query will map to in mongo shell.
# http://docs.mongodb.org/manual/reference/sql-comparison/
# http://www.querymongo.com/
#


def sql_to_mongo(query):
    """Convert an SQL query to a mongo spec.
    This only supports select statements. For now.
    :param query: String. A SQL query.
    :return: None or a dictionary containing a mongo spec.

    Parameters
    ----------
    query : str
        SQL query

    Returns
    -------
    dict
        dictionary containing the MongoDB query
    """

    def fix_token_list(in_list):
        """
        tokens as List is some times deeply nested and hard to deal with.
        Improve parser grouping remove this.
        """
        if isinstance(in_list, list) and len(in_list) == 1 and isinstance(in_list[0], list):
            return fix_token_list(in_list[0])
        else:
            return [item for item in in_list]

    def select_count_func(tokens=None):
        return full_select_func(tokens, "count")

    def select_distinct_func(tokens=None):
        return full_select_func(tokens, "distinct")

    def select_func(tokens=None):
        return full_select_func(tokens, "select")

    def full_select_func(tokens=None, method="select"):
        """
        Take tokens and return a dictionary.
        """
        action = {"distinct": "distinct", "count": "count"}.get(method, "find")
        if tokens is None:
            return
        ret = {action: True, "fields": {item: 1 for item in fix_token_list(tokens.asList())}}
        if ret["fields"].get("id"):  # Use _id and not id
            # Drop _id from fields since mongo always return _id
            del ret["fields"]["id"]
        else:
            ret["fields"]["_id"] = 0
        if "*" in ret["fields"].keys():
            ret["fields"] = {}
        return ret

    def where_func(tokens=None):
        """
        Take tokens and return a dictionary.
        """
        if tokens is None:
            return

        tokens = fix_token_list(tokens.asList()) + [None, None, None]
        cond = {"!=": "$ne", ">": "$gt", ">=": "$gte", "<": "$lt", "<=": "$lte", "like": "$regex"}.get(tokens[1])

        find_value = tokens[2].strip('"').strip("'")
        if cond == "$regex":
            if find_value[0] != "%":
                find_value = "^" + find_value
            if find_value[-1] != "%":
                find_value = find_value + "$"
            find_value = find_value.strip("%")

        if cond is None:
            expr = {tokens[0]: find_value}
        else:
            expr = {tokens[0]: {cond: find_value}}

        return expr

    def combine(tokens=None):
        if tokens:
            tokens = fix_token_list(tokens.asList())
            if len(tokens) == 1:
                return tokens
            else:
                return {"${}".format(tokens[1]): [tokens[0], tokens[2]]}

    # TODO: Reduce list of imported functions.
    from pyparsing import (
        Word,
        alphas,
        CaselessKeyword,
        Group,
        Optional,
        ZeroOrMore,
        Forward,
        Suppress,
        alphanums,
        OneOrMore,
        quotedString,
        Combine,
        Keyword,
        Literal,
        replaceWith,
        oneOf,
        nums,
        removeQuotes,
        QuotedString,
        Dict,
    )

    LPAREN, RPAREN = map(Suppress, "()")
    EXPLAIN = CaselessKeyword("EXPLAIN").setParseAction(lambda t: {"explain": True})
    SELECT = Suppress(CaselessKeyword("SELECT"))
    WHERE = Suppress(CaselessKeyword("WHERE"))
    FROM = Suppress(CaselessKeyword("FROM"))
    CONDITIONS = oneOf("= != < > <= >= like", caseless=True)
    AND = CaselessKeyword("and")
    OR = CaselessKeyword("or")

    word_match = Word(alphanums + "._") | quotedString
    number = Word(nums)
    statement = Group(word_match + CONDITIONS + word_match).setParseAction(where_func)
    select_fields = Group(
        SELECT + (word_match | Keyword("*")) + ZeroOrMore(Suppress(",") + (word_match | Keyword("*")))
    ).setParseAction(select_func)

    select_distinct = (
        SELECT
        + Suppress(CaselessKeyword("DISTINCT"))
        + LPAREN
        + (word_match | Keyword("*"))
        + ZeroOrMore(Suppress(",") + (word_match | Keyword("*")))
        + Suppress(RPAREN)
    ).setParseAction(select_distinct_func)

    select_count = (
        SELECT
        + Suppress(CaselessKeyword("COUNT"))
        + LPAREN
        + (word_match | Keyword("*"))
        + ZeroOrMore(Suppress(",") + (word_match | Keyword("*")))
        + Suppress(RPAREN)
    ).setParseAction(select_count_func)
    LIMIT = (Suppress(CaselessKeyword("LIMIT")) + word_match).setParseAction(lambda t: {"limit": t[0]})
    SKIP = (Suppress(CaselessKeyword("SKIP")) + word_match).setParseAction(lambda t: {"skip": t[0]})
    from_table = (FROM + word_match).setParseAction(lambda t: {"collection": t.asList()[0]})
    # word = ~(AND | OR) + word_match

    operation_term = (
        select_distinct | select_count | select_fields
    )  # place holder for other SQL statements. ALTER, UPDATE, INSERT
    expr = Forward()
    atom = statement | (LPAREN + expr + RPAREN)
    and_term = (OneOrMore(atom) + ZeroOrMore(AND + atom)).setParseAction(combine)
    or_term = (and_term + ZeroOrMore(OR + and_term)).setParseAction(combine)

    where_clause = (WHERE + or_term).setParseAction(lambda t: {"spec": t[0]})
    list_term = (
        Optional(EXPLAIN) + operation_term + from_table + Optional(where_clause) + Optional(LIMIT) + Optional(SKIP)
    )
    expr << list_term

    ret = expr.parseString(query.strip())
    spec = ret[2]["spec"]

    query_dict = {}
    _ = map(query_dict.update, ret)
    # return query_dict
    return spec


def spec_str(spec):
    """
    Change a spec to the json object format used in mongo.
    eg. Print dict in python gives: {'a':'b'}
        mongo shell would do {a:'b'}
        Mongo shell can handle both formats but it looks more like the
        official docs to keep to their standard.
    :param spec: Dictionary. A mongo spec.
    :return: String. The spec as it is represended in the mongodb shell examples.
    """

    if spec is None:
        return "{}"
    if isinstance(spec, list):
        out_str = "[" + ", ".join([spec_str(x) for x in spec]) + "]"
    elif isinstance(spec, dict):
        out_str = "{" + ", ".join(["{}:{}".format(x, spec_str(spec[x])) for x in sorted(spec)]) + "}"
    elif spec and isinstance(spec, str) and not spec.isdigit():
        out_str = "'" + spec + "'"
    else:
        out_str = spec

    return out_str
