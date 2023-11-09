import pytest

#
# Testing sanitize_string function in common.py library
#

from dtaas.tuilib.common import sanitize_string


def test_asterisk():
    """
    Testing that asterisk is correctly escaped
    """
    string = r"SELECT * FROM metadata"

    assert sanitize_string(version="client", string=string) == r"SELECT \* FROM metadata"
    assert sanitize_string(version="server", string=string) == r"SELECT \* FROM metadata"


def test_single_quotes():
    """
    Testing that single quotes are correctly escaped
    """
    string = r"SELECT * FROM metadata where category = 'motorcycle'"

    assert (
        sanitize_string(version="client", string=string)
        == r"SELECT \* FROM metadata where category = \'motorcycle\'"
    )
    assert (
        sanitize_string(version="server", string=string)
        == r"SELECT \* FROM metadata where category = \\\'motorcycle\\\'"
    )


def test_quoted_argument_double():
    """
    Testing that double quotes are correctly escaped
    """
    string = r'SELECT * FROM metadata WHERE width > "600"'

    assert (
        sanitize_string(version="client", string=string)
        == r"SELECT \* FROM metadata WHERE width > \"600\""
    )
    assert (
        sanitize_string(version="server", string=string)
        == r"SELECT \* FROM metadata WHERE width > \\\"600\\\""
    )


def test_parentheses():
    """
    Testing that parentheses are correctly escaped
    """
    string = r"def main(files_in):"

    assert sanitize_string(version="client", string=string) == r"def main\(files_in\):"
    assert sanitize_string(version="server", string=string) == r"def main\(files_in\):"


def test_newline():
    """
    Testing that newline character is correctly escaped
    """
    string = r"def main(files_in):\n return files_in"

    assert (
        sanitize_string(version="client", string=string)
        == r"def main\(files_in\):\\n return files_in"
    )
    assert (
        sanitize_string(version="server", string=string)
        == r"def main\(files_in\):\\\\n return files_in"
    )


def test_wrong_version():
    """
    Test that entering a wrong name version raises an exception.
    """
    with pytest.raises(NameError):
        sanitize_string(version="test", string="")
