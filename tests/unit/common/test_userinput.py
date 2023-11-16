import pytest

#
# Testing UserInput class in common.py library
#

import os, json
from dtaas.tuilib.common import UserInput


def test_sql_only():
    """
    Test initialization of UserInput class with only SQL query
    """

    data = {
        "id": "42",
        "sql_query": "SELECT * FROM metadata WHERE category = 'motorcycle'",
    }
    user_input = UserInput(data)

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script_path == None


def test_sql_and_script():
    """
    Test initialization of UserInput class with only SQL query
    """

    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out")

    data = {
        "id": "42",
        "sql_query": "SELECT * FROM metadata WHERE category = 'motorcycle'",
        "script_path": "user_script.py",
    }
    user_input = UserInput(data)

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script_path == "user_script.py"
    with open(user_input.script_path, "r") as f:
        assert f.read() == "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"


def test_missing_query():
    """
    Test that the initialization fails if query is not provided
    """
    with pytest.raises(KeyError):
        data = {"id": "42"}
        UserInput(data)


def test_missing_query():
    """
    Test that the initialization fails if ID is not provided
    """
    with pytest.raises(KeyError):
        data = {"sql_query": "SELECT * FROM metadata WHERE category = 'motorcycle'"}
        UserInput(data)


def test_cli_input_sql(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (no script)
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            '{"id": "42", "sql_query": "SELECT * FROM metadata WHERE ID = 123456"}',
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE ID = 123456"
    assert user_input.script_path == None


@pytest.mark.xfail  # TODO: evaluate converting all double quotes to single quotes
def test_cli_input_sql_double_quotes(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (no script) using double quotes
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            '{"id": "42", "sql_query": "SELECT * FROM metadata WHERE category = "motorcycle""}',
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.id == "42"
    assert user_input.sql_query == 'SELECT * FROM metadata WHERE category = "motorcycle"'
    assert user_input.script_path == None


def test_cli_input_sql_single_quotes(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (no script) using single quotes
    """

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            """{"id": "42", "sql_query": "SELECT * FROM metadata WHERE category = 'motorcycle'"}""",
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script_path == None


def test_cli_input_script(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (with script)
    """

    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out")

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            '{"id": "42", "sql_query": "SELECT * FROM metadata WHERE ID = 123456", "script_path": "user_script.py"}',
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE ID = 123456"
    assert user_input.script_path == "user_script.py"
    with open(user_input.script_path, "r") as f:
        assert f.read() == "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"


@pytest.mark.xfail  # TODO: evaluate converting all double quotes to single quotes
def test_cli_input_script_double_quotes(monkeypatch):
    """
    Test initialization of UserInput class from command-line input (with double quotes in script)
    """

    with open("user_script.py", "r") as f:
        f.write(
            'def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print("DONE!")\n return files_out'
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            '{"id": 42, "sql_query": "SELECT * FROM metadata WHERE ID = 123456", "script_path": "user_script.py"}',
        ],
    )

    user_input = UserInput.from_cli()

    assert user_input.sql_query == "SELECT * FROM metadata WHERE ID = 123456"
    assert user_input.script == "user_script.py"
    assert user_input.id == 42
    with open(user_input.script_path, "r") as f:
        assert (
            f.read()
            == 'def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print("DONE!")\n return files_out'
        )


def test_config():
    """
    Test initialization of UserInput class with only SQL query and custom config
    """

    data = {
        "id": 42,
        "sql_query": "SELECT * FROM metadata WHERE category = 'motorcycle'",
        "config_client": {"ip": "localhost"},
    }
    user_input = UserInput(data)

    assert user_input.id == 42
    assert user_input.sql_query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script_path == None
    assert user_input.config_client == {"ip": "localhost"}


def test_json_input_sql(monkeypatch):
    """
    Test initialization of UserInput class from JSON file input
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM metadata WHERE ID = 123456",
            },
            f,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            "input.json",
        ],
    )

    user_input = UserInput.from_json(json_path="input.json")

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE ID = 123456"
    assert user_input.script_path == None


def test_json_input_sql_and_script(monkeypatch):
    """
    Test initialization of UserInput class from JSON file input + script
    """

    with open("user_script.py", "w") as f:
        f.write("def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out")

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM metadata WHERE ID = 123456",
                "script_path": "user_script.py",
            },
            f,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            "input.json",
        ],
    )

    user_input = UserInput.from_json(json_path="input.json")

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE ID = 123456"
    assert user_input.script_path == "user_script.py"
    with open(user_input.script_path, "r") as f:
        assert f.read() == "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n return files_out"


def test_json_input_with_single_quotes(monkeypatch):
    """
    Test initialization of UserInput class from JSON file input + script using single quotes
    """

    with open("user_script.py", "w") as f:
        f.write(
            "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print('DONE!')\n return files_out"
        )

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": "SELECT * FROM metadata WHERE category = 'motorcycle'",
                "script_path": "user_script.py",
            },
            f,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            "input.json",
        ],
    )

    user_input = UserInput.from_json(json_path="input.json")

    assert user_input.id == "42"
    assert user_input.sql_query == "SELECT * FROM metadata WHERE category = 'motorcycle'"
    assert user_input.script_path == "user_script.py"
    with open(user_input.script_path, "r") as f:
        assert (
            f.read()
            == "def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print('DONE!')\n return files_out"
        )


def test_json_input_with_double_quotes(monkeypatch):
    """
    Test initialization of UserInput class from JSON file input + script using double quotes
    """

    with open("user_script.py", "w") as f:
        f.write(
            'def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print("DONE!")\n return files_out'
        )

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": 'SELECT * FROM metadata WHERE category = "motorcycle"',
                "script_path": "user_script.py",
            },
            f,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            "input.json",
        ],
    )

    user_input = UserInput.from_json(json_path="input.json")

    assert user_input.id == "42"
    assert user_input.sql_query == 'SELECT * FROM metadata WHERE category = "motorcycle"'
    assert user_input.script_path == "user_script.py"
    with open(user_input.script_path, "r") as f:
        assert (
            f.read()
            == 'def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print("DONE!")\n return files_out'
        )


def test_json_input_with_multiple_quotes(monkeypatch):
    """
    Test initialization of UserInput class from JSON file input + script using a mix of single and double quotes
    """

    with open("user_script.py", "w") as f:
        f.write(
            """def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print('DONE!')\n print("DOUBLE DONE!")\n return files_out"""
        )

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": """SELECT * FROM metadata WHERE category = "motorcycle" OR category = 'hotdog'""",
                "script_path": "user_script.py",
            },
            f,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            "input.json",
        ],
    )

    user_input = UserInput.from_json(json_path="input.json")

    assert user_input.id == "42"
    assert user_input.sql_query == """SELECT * FROM metadata WHERE category = "motorcycle" OR category = 'hotdog'"""
    assert user_input.script_path == "user_script.py"
    with open(user_input.script_path, "r") as f:
        assert (
            f.read()
            == """def main(files_in):\n files_out=files_in.copy()\n files_out.reverse()\n print('DONE!')\n print("DOUBLE DONE!")\n return files_out"""
        )


def test_json_input_config(monkeypatch):
    """
    Test initialization of UserInput class from JSON file input containing configuration info
    """

    with open("input.json", "w") as f:
        json.dump(
            {
                "id": "42",
                "sql_query": """SELECT * FROM metadata WHERE category = "motorcycle" OR category = 'hotdog'""",
                "config_server": {"walltime": "01:00:00", "ntasks_per_node": 48},
            },
            f,
        )

    monkeypatch.setattr(
        "sys.argv",
        [
            f"{os.path.dirname(os.path.abspath(__file__))}/../../../dtaas/bin/dtaas_tui_client.py",
            "input.json",
        ],
    )

    user_input = UserInput.from_json(json_path="input.json")

    assert user_input.id == "42"
    assert user_input.sql_query == """SELECT * FROM metadata WHERE category = "motorcycle" OR category = 'hotdog'"""
    assert user_input.script_path == None
    assert user_input.config_server == {"walltime": "01:00:00", "ntasks_per_node": 48}
