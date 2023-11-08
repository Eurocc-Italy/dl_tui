import pytest

#
# Testing UserInput class in common.py library
#


from dtaas.lib.common import UserInput


def test_sql_only():
    """
    Test initialization of UserInput class with only SQL query
    """

    data = {
        "query": "SELECT * FROM metadata WHERE category = 'motorcycle'",
        "ID": 42,
    }
    user_input = UserInput(data)

    assert user_input.query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script == None
    assert user_input.id == 42


def test_sql_and_script():
    """
    Test initialization of UserInput class with only SQL query
    """

    data = {
        "query": "SELECT * FROM metadata WHERE category = 'motorcycle'",
        "script": "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out",
        "ID": 42,
    }
    user_input = UserInput(data)

    assert user_input.query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert (
        user_input.script
        == "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"
    )
    assert user_input.id == 42


def test_missing_query():
    """
    Test that the initialization fails if query is not provided
    """
    with pytest.raises(KeyError):
        data = {"ID": 42}
        UserInput(data)


def test_missing_query():
    """
    Test that the initialization fails if ID is not provided
    """
    with pytest.raises(KeyError):
        data = {"query": "SELECT * FROM metadata WHERE category = 'motorcycle'"}
        UserInput(data)


def test_cli_input_sql(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (no script)
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            "dtaas_tui_client",
            """{\"query\": \"SELECT * FROM metadata WHERE ID = 123456\", \"ID\": 42}""",
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.query == "SELECT * FROM metadata WHERE ID = 123456"
    assert user_input.script == None
    assert user_input.id == 42


@pytest.mark.xfail  # TODO: evaluate converting all double quotes to single quotes
def test_cli_input_sql_double_quotes(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (no script) using double quotes
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            "dtaas_tui_client",
            """{\"query\": \"SELECT * FROM metadata WHERE category = \"motorcycle\"\", \"ID\": 42}""",
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script == None
    assert user_input.id == 42


def test_cli_input_sql_single_quotes(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (no script) using single quotes
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            "dtaas_tui_client",
            """{\"query\": \"SELECT * FROM metadata WHERE category = \'motorcycle\'\", \"ID\": 42}""",
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script == None
    assert user_input.id == 42


def test_cli_input_script(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (with script)
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            "dtaas_tui_client",
            """{\"ID\": 42, \"query\": \"SELECT * FROM metadata WHERE ID = 123456\", \"script\": \"def main(files_in):\\n files_out=files_in.copy()\\n files_out.reverse()\\n return files_out\"}""",
        ],
    )

    import sys

    print(sys.argv)

    user_input = UserInput.from_cli()

    assert user_input.query == "SELECT * FROM metadata WHERE ID = 123456"
    assert (
        user_input.script
        == "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"
    )
    assert user_input.id == 42


@pytest.mark.xfail  # TODO: evaluate converting all double quotes to single quotes
def test_cli_input_script_double_quotes(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (with double quotes in script)
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            "dtaas_tui_client",
            """{\"ID\": 42, \"query\": \"SELECT * FROM metadata WHERE ID = 123456\", \"script\": \"def main(files_in):\\n files_out=files_in.copy()\\n files_out.reverse()\\n print(\"DONE!\")\n return files_out\"}""",
        ],
    )

    import sys

    print(sys.argv)

    user_input = UserInput.from_cli()

    assert user_input.query == "SELECT * FROM metadata WHERE ID = 123456"
    assert (
        user_input.script
        == 'def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print("DONE!")\n return files_out'
    )
    assert user_input.id == 42
