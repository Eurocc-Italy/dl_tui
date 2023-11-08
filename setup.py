from setuptools import setup

setup(
    name="DTaaS",
    version="0.0.1",
    description="Query/Process interface for DTaaS",
    packages=["dtaas"],
    package_data={
        "dtaas": ["*", "bin", "tuilib", "etc"],
    },
    scripts=["dtaas/bin/dtaas_tui_client", "dtaas/bin/dtaas_tui_server"],
    install_requires=[],
)
