from setuptools import setup, find_packages

setup(
    name="dl-tui",
    version="0.4.1",
    description="Text user interface for Cineca's Data Lake as a Service",
    packages=find_packages(include=["dlaas", "dlaas.*"]),
    package_data={
        "dlaas": [
            "etc/*",
            "etc/default/*",
        ],
    },
    entry_points={
        "console_scripts": [
            "dl_tui=dlaas.bin.dl_tui:main",
            "dl_tui_hpc=dlaas.bin.dl_tui_hpc:main",
            "dl_tui_server=dlaas.bin.dl_tui_server:main",
        ],
    },
    install_requires=[
        "sh",
        "wheel",
        "pymongo==4.5.0",
        "python-dateutil==2.6.0",
        "urllib3==1.26",
        "pyparsing",
        "sqlparse @ git+https://github.com/lbabetto/sqlparse",
        "boto3",
        "requests",
    ],
    author="Luca Babetto",
    author_email="l.babetto@cineca.it",
)
