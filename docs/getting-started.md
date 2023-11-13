(getting-started)=

# Getting Started

## Installation

We highly recommend installing the software in a custom Python virtual environment. You can set up a virtual environment with third-party tools such as [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html) or with the built-in [venv](https://docs.python.org/3/library/venv.html) module.

After having set up and activated your virtual environment, follow these steps to install the DTaaS TUI:

  1. Download the [DTaaS_TUI](https://gitlab.hpc.cineca.it/lbabetto/DTaaS_TUI) repository:
  
  ```bash
  git clone https://gitlab.hpc.cineca.it/lbabetto/DTaaS_TUI
  ```

  2. Install the requirements: 
  
  ```bash
  pip install -r DTaaS_TUI/requirements.txt
  ```
  
  3. Install the custom version of the [sqlparse](https://github.com/lbabetto/sqlparse) library (currently pending a pull request to the original repository):

  ```bash
  git clone https://github.com/lbabetto/sqlparse
  cd sqlparse
  pip install sqlparse/
  ```

  4. Install the library: 
  
  ```bash
  pip install DTaaS_TUI/
  ```

## Configuration

Configuration options can be saved in the library `/etc` folder as JSON files. For client/server versions, these should be named `config_client.json` and `config_server.json`, respectively. The library first loads the default options written the JSON files located in `/etc/default` (which can be taken as a template to understand the kind of options which can be configured) and overwrites them with the content of the JSON files in `/etc`.

For the client version, the configurable options are:

  * `user`: the user name in the MongoDB server
  * `password`: the password of the MongoDB server
  * `ip`: network address of the machine running the MongoDB server
  * `port`: the port to access the MongoDB server
  * `database`: the name of the MongoDB database
  * `collection`: the name of the MongoDB collection within the database

For the server version, the configurable options are:

  * `user`: username of the HPC account
  * `host`: address of the HPC login node
  * `venv_path`: path of the virtual environment in which the library is installed
  * `ssh_key`: path to the SSH key used for authentication on the HPC login node
  * `partition`: SLURM partition for the HPC job
  * `account`: SLURM account for the HPC job
  * `mail`: email address to which the notifications for job start/end are sent
  * `walltime`: maximum walltime for HPC job
  * `nodes`: number of nodes requested for HPC job
  * `ntasks_per_node`: number of CPU cores per node requested for the HPC job

