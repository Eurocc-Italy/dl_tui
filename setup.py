from setuptools import setup

setup(
    name="DTaaS",
    version="0.0.1",
    description="Query/Process interface for DTaaS",
    packages=["dtaas"],
    package_data={
        "bin": ["*"],
        "etc": ["*"],
        "tuilib": ["*"],
    },
    scripts=["bin/dtaas_tui_client", "bin/dtaas_tui_server"]
    install_requires=[],
)
