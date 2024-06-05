import pytest

#
# Testing sanitize function in Config class
#

from dlaas.tuilib.common import Config
from dlaas.tuilib.common import sanitize_dictionary


def test_server_illegal_characters():
    """Test that if an illegal character is used the program throws an exception"""
    config = Config("server")

    bad_keys = [
        "05:00:00 `curl abc.def.com`",
        "login.g100.cineca.it curl$(IFS)abc.def.com",
    ]

    for key in bad_keys:
        for item in config.__dict__:
            with pytest.raises(SyntaxError):
                config.__dict__[item] = key
                sanitize_dictionary(config.__dict__)


# Server config
def test_server_user():
    """Test that the "user" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "m-rossi",
        "m rossi",
        "m.rossi",
        "mrossi 123",
        "mrossi$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["user"] = key
            sanitize_dictionary(config.__dict__)


def test_server_host():
    """Test that the "host" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "123.456.789.",
        "login02-ext.g100.cineca.it test",
        "login02-ext.g100.cineca.it.",
        "123.456.789$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["host"] = key
            sanitize_dictionary(config.__dict__)


def test_server_venv_path():
    """Test that the "venv_path" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "home/mrossi",
        "~/dtaas_venv bash",
        "/home/mrossi$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["venv_path"] = key
            sanitize_dictionary(config.__dict__)


def test_server_ssh_key():
    """Test that the "ssh_key" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "~/.ssh/mario-hpc test",
        "bash ~/.ssh/mario-hpc",
        "~/.ssh/mario-hpc$(IFS)$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["ssh_key"] = key
            sanitize_dictionary(config.__dict__)


def test_server_mail():
    """Test that the "mail" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "m.rossi-45@cineca.it",
        "m.rossi+123@cineca.it",
        "m.rossicineca.it",
        "m.rossi-45@cineca.it bash",
        "m.rossi@cineca.it$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["mail"] = key
            sanitize_dictionary(config.__dict__)


def test_server_walltime():
    """Test that the "walltime" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "test12-34:56:78",
        "12-34:56:78 bash",
        "12-34:56:78$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["walltime"] = key
            sanitize_dictionary(config.__dict__)


def test_server_nodes():
    """Test that the "nodes" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "test12",
        "12 bash",
        "12-34",
        "56:78",
        "34:56:78",
        "3km",
        "2n",
        "12$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["nodes"] = key
            sanitize_dictionary(config.__dict__)


def test_server_ntasks_per_node():
    """Test that the "ntasks_per_node" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "test12",
        "12 bash",
        "12-34",
        "56:78",
        "34:56:78",
        "12$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["ntasks_per_node"] = key
            sanitize_dictionary(config.__dict__)


# HPC config
def test_hpc_user():
    """Test that the "user" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "test abc",
        "pippo$(IFS)",
        "mario$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["user"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_password():
    """Test that the "password" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "test abc",
        "test `bash 123`",
        "password$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["password"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_ip():
    """Test that the "ip" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "123.456.678.90 123",
        "login02`-ext.g100.cineca.it",
        "123.456.678.90$",
        "123.456.678.90$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["ip"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_port():
    """Test that the "port" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "abc",
        "2$",
        "28ht",
        "27017$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["port"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_database():
    """Test that the "database" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "2$",
        "e test",
        "test$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["database"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_collection():
    """Test that the "collection" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "2$",
        "e test",
        "test$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["collection"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_s3_endpoint_url():
    """Test that the "s3_endpoint_url" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "s3ds.g100st.cineca.it t4e",
        "s3ds.g100st.cineca.it.",
        "httpas://s3ds.g100st.cineca.it/",
        "https://s3ds.g100st.cineca.it.",
        "https://s3ds.g100st.cineca.it$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["s3_endpoint_url"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_s3_bucket():
    """Test that the "s3_bucket" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "2$",
        "e test",
        "test$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["s3_bucket"] = key
            sanitize_dictionary(config.__dict__)


def test_hpc_pfs_prefix_path():
    """Test that the "pfs_prefix_path" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "/g100_s3/DRES_s3poc/../OTHERONE",
        "./g100_s3/DRES_s3poc",
        "/g100_s3/DRES_s3poc test",
        "/g100_s3/DRES_s3poc``",
        "/g100s3/DRES_s3poc$(IFS)`curl abc.def.com`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["pfs_prefix_path"] = key
            sanitize_dictionary(config.__dict__)
