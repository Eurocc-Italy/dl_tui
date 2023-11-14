from setuptools import setup, find_packages

setup(
    name="DTaaS_TUI",
    version="0.1",
    description="Query/Process interface for DTaaS",
    packages=find_packages(include=["dtaas"]),
    package_data={
        "dtaas": ["*", "bin/*", "tuilib/*", "etc/*"],
    },
    scripts=["dtaas/bin/dtaas_tui_client", "dtaas/bin/dtaas_tui_server"],
    install_requires=[],
    author="Luca Babetto",
    author_email="l.babetto@cineca.it",
)
