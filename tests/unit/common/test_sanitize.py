import pytest

#
# Testing sanitize function in Config class
#

from dlaas.tuilib.common import Config


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
                config.sanitize()


# Server config
def test_server_user():
    """Test that the "user" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "m-rossi",
        "m rossi",
        "m.rossi",
        "mrossi 123",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["user"] = key
            config.sanitize()


def test_server_host():
    """Test that the "host" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "123.456.789.",
        "login02-ext.g100.cineca.it test",
        "login02-ext.g100.cineca.it.",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["host"] = key
            config.sanitize()


def test_server_venv_path():
    """Test that the "venv_path" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "home/lbabetto",
        "~/dtaas_venv bash",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["venv_path"] = key
            config.sanitize()


def test_server_ssh_key():
    """Test that the "ssh_key" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "~/.ssh/luca-hpc test",
        "bash ~/.ssh/luca-hpc",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["ssh_key"] = key
            config.sanitize()


def test_server_mail():
    """Test that the "mail" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "m.rossi-45@cineca.it",
        "m.rossi+123@cineca.it",
        "m.rossicineca.it",
        "m.rossi-45@cineca.it bash",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["mail"] = key
            config.sanitize()


def test_server_walltime():
    """Test that the "walltime" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "test12-34:56:78",
        "12-34:56:78 bash",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["walltime"] = key
            config.sanitize()


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
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["nodes"] = key
            config.sanitize()


def test_server_ntasks_per_node():
    """Test that the "ntasks_per_node" keyword in Server config does not accept incorrect syntax"""
    config = Config("server")

    bad_keys = [
        "test12",
        "12 bash",
        "12-34",
        "56:78",
        "34:56:78",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["ntasks_per_node"] = key
            config.sanitize()


# HPC config
def test_hpc_user():
    """Test that the "user" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "test abc",
        "pippo$(IFS)",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["user"] = key
            config.sanitize()


def test_hpc_password():
    """Test that the "password" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "test abc",
        "test `bash 123`",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["password"] = key
            config.sanitize()


def test_hpc_ip():
    """Test that the "ip" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "131.175.205.87 123",
        "login02`-ext.g100.cineca.it",
        "131.175.205.87$",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["ip"] = key
            config.sanitize()


def test_hpc_port():
    """Test that the "port" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "abc",
        "2$",
        "28ht",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["port"] = key
            config.sanitize()


def test_hpc_database():
    """Test that the "database" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "2$",
        "e test",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["database"] = key
            config.sanitize()


def test_hpc_collection():
    """Test that the "collection" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "2$",
        "e test",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["collection"] = key
            config.sanitize()


def test_hpc_s3_endpoint_url():
    """Test that the "s3_endpoint_url" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "s3ds.g100st.cineca.it t4e",
        "s3ds.g100st.cineca.it.",
        "httpas://s3ds.g100st.cineca.it/",
        "https://s3ds.g100st.cineca.it.",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["s3_endpoint_url"] = key
            config.sanitize()


def test_hpc_s3_bucket():
    """Test that the "s3_bucket" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "2$",
        "e test",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["s3_bucket"] = key
            config.sanitize()


def test_hpc_pfs_prefix_path():
    """Test that the "pfs_prefix_path" keyword in HPC config does not accept incorrect syntax"""
    config = Config("hpc")

    bad_keys = [
        "/g100_s3/DRES_s3poc/../OTHERONE",
        "./g100_s3/DRES_s3poc",
        "/g100_s3/DRES_s3poc test",
        "/g100_s3/DRES_s3poc``",
    ]

    for key in bad_keys:
        with pytest.raises(SyntaxError):
            config.__dict__["pfs_prefix_path"] = key
            config.sanitize()
