(getting-started)=

# Getting Started

## Installation

We highly recommend installing the software in a custom Python virtual environment. You can set up a virtual environment with third-party tools such as [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html) or with the built-in [venv](https://docs.python.org/3/library/venv.html) module.

After having set up and activated your virtual environment, follow these steps to install the DTaaS TUI:

  1. Install the custom version of the [sqlparse](https://github.com/lbabetto/sqlparse) library (currently pending a pull request to the original repository):

  ```bash
  git clone https://github.com/lbabetto/sqlparse
  pip install -r sqlparse/requirements.txt
  pip install sqlparse/
  ```
  
  2. Download and install the [DTaaS_TUI](https://gitlab.hpc.cineca.it/lbabetto/DTaaS_TUI) repository and its requirements:
  
  ```bash
  git clone https://gitlab.hpc.cineca.it/lbabetto/DTaaS_TUI  
  pip install -r DTaaS_TUI/requirements.txt
  pip install DTaaS_TUI/
  ```
