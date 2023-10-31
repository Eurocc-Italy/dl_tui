from setuptools import setup

setup(
    name="DTaaS",
    version="0.0.1",
    description="Query/Process interface for DTaaS",
    packages=["dtaas"],
    package_data={
        "dtaas": [
            "*",
        ],
    },
    install_requires=[],
)
