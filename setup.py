from setuptools import setup

setup(
    name="DTaaS",
    version="0.0.1",
    description="TUI for DTaaS",
    packages=["tui"],
    package_data={
        "tui": [
            "*",
            "core/*",
            "interface/*",
        ],
    },
    scripts=["tui/interface/tui"],
    install_requires=[],
)
