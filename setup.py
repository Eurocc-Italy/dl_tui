from setuptools import setup, find_packages

setup(
    name="DTaaS_TUI",
    version="0.1",
    description="Query/Process interface for DTaaS",
    packages=find_packages(include=["dtaas"]),
    package_data={
        "dtaas": [
            "etc/*",
            "etc/default/*",
        ],
    },
    data_files=[
        (
            "config",
            [
                "dtaas/etc/default/config_client.json",
                "dtaas/etc/default/config_server.json",
            ],
        ),
    ],
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
