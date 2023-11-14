from setuptools import setup, find_packages

setup(
    name="DTaaS_TUI",
    version="0.2",
    description="Query/Process interface for DTaaS",
    packages=find_packages(include=["dtaas", "dtaas.*"]),
    package_data={
        "dtaas": [
            "etc/*",
            "etc/default/*",
        ],
    },
    entry_points={
        "console_scripts": [
            "dtaas_tui_client=dtaas.bin.dtaas_tui_client:main",
            "dtaas_tui_server=dtaas.bin.dtaas_tui_server:main",
        ],
    },
    install_requires=[],
    author="Luca Babetto",
    author_email="l.babetto@cineca.it",
)
