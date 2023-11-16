# DTaaS-TUI

Query/processing text user interface for the DTaaS project.

Consists of two executables, `dtaas_tui_client` and `dtaas_tui_server`, which take as argument the name of a JSON-formatted document with the following keys:

 - `id`: a unique identifier (we recommend using the [UUID](https://docs.python.org/3/library/uuid.html) module to generate it, producing a UUID.hex string)
 - `sql_query`: an SQL query to be run on the MongoDB database containing the metadata 
 - `script_path` (optional): the path to a Python script to analyse the files matching the query
 - `config_client` (optional): a dictionary containing options for client-side configuration (vide infra)
 - `config_server` (optional): a dictionary containing options for server-side configuration (vide infra)

The Python script should contain a `main` function, which takes a list of file paths as input, processes the data, and returns a list of file paths as output. The interface then runs the script and saves the resulting files in a compressed archive, which is ultimately accessible by the user.

A typical script should look something like this:

```
<useful imports>
...
<function definitions>
...

def main(input_files):
    output files = []
    ...
    <processing input_files -> populating output_files>
    ...
    return output_files
```

If the `dtaas_tui_client` version is called, the interface will run locally (still accessing a remote MongoDB server if configured as such), therefore files should be available on the local machine.

If the `dtaas_tui_server` version is called, the interface will launch a job on HPC, where the processing will run on the compute nodes, accessing the files on the parallel filesystem.

If no script is provided, the results will simply be the files corresponding to the query.

## Installing

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

## Configuration

Configuration options can be given via the `config_client` and `config_server` keys in the input JSON file. The library first loads the default options written the JSON files located in `/etc/default` (which can be taken as a template to understand the kind of options which can be configured) and overwrite with the content provided in the input JSON file.

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
