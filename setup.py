from setuptools import setup, find_packages

setup(
    name="dl_tui",
    version="1.2",
    description="Text user interface for EuroCC-Italy's Data Lake Ready to Use",
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
        "pymongo==4.6.3",
        "boto3==1.35.0",
        "python-dateutil==2.7.0",
        "urllib3==1.26.19",
        "pyparsing",
        "sqlparse @ git+https://github.com/lbabetto/sqlparse",
        "requests",
    ],
    author="Luca Babetto",
    author_email="l.babetto@cineca.it",
)
