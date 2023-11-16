# DTaaS-TUI

This is the user guide for the Digital Twin as a Service Text User Interface (`DTaaS_TUI`) Python library. The TUI is used to query the data lake and run processing scripts on the files matching the query.

The service is composed of: 

  - a Cloud-based infrastructure with a VM running a MongoDB database instance. This database contains the metadata for all the files in the data lake / digital twin, including the path to the actual corresponding file in the HPC parallel filesystem.
  - a HPC cluster which contains the actual files, stored in dual parallel filesystem/S3 mode. The HPC part runs the processing script sent by the user on the files matching the query.

The library consists of two executables, `dtaas_tui_client` and `dtaas_tui_server`. 

The `client` version is intended to be run on the machine with direct access to the files of the data lake and runs the actual querying and processing.

The `server` version is intended to be run on a cloud machine with access to an HPC infrastructure running Slurm. Its purpose is to parse the user input (query and processing script) and launch a Slurm job on HPC which calls the client version.

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

## Preparing the input

The input argument of the executable should be a properly-formatted JSON document, whose content should be the following:

  - `id`: unique identifier characterizing the run. We recommend using the [UUID](https://docs.python.org/3/library/uuid.html) module to generate it, producing a UUID.hex string
  - `sql_query`: SQL query to be run on the MongoDB database containing the metadata. Since the data lake is by definition a non-relational database, and the "return" of the query is the file itself, most queries will be of the type `SELECT * FROM metadata WHERE [...]`.
  - `script_path` (optional): path to a Python script to analyse the files matching the query. This script must feature a `main` function taking as input a list of file paths, which will be populated by the interface with the files matching the query, and returning a list of paths as output, which will be saved in a compressed archive and made available to the user.
  - `config_client` (optional): a dictionary containing options for client-side configuration
  - `config_server` (optional): a dictionary containing options for server-side configuration

After you prepared the JSON file (for example, called `input.json`), the program can be called as such:

```
dtaas_tui_<client/server> input.json
```

If no script is provided, the program will simply return the files matching the query.

## Running scripts

Let's say you want to fetch a certain set of files from the database and run a Python analysis script on them.

The Python script must have a `main` function, which will be what is actually run by the executable. This function should take a list as input, which will be populated by the interface with the paths of the files matching the SQL query. The function should also return a list as output, which contains the paths of the files which the user wants to save from the analysis. The interface will then take this list of paths, save the corresponding files (generated _in situ_ on HPC) in an archive and make them available to the user for download via the API/GUI.

This is an example of a valid Python script to be passed to the interface, with a `main` function taking a list of paths as input and returning a list of paths as output:

```python
import imageio as iio
import skimage as ski
import matplotlib.pyplot as plt
import os, sys
from multiprocessing import Pool

def rotate_image(img_path):
    os.makedirs('rotated', exist_ok=True)

    image = iio.imread(uri=img_path)
    rotated = ski.transform.rotate(image=image, angle=45)

    fig, ax = plt.subplots(1,2)
    ax[0].imshow(image)
    ax[1].imshow(rotated)

    path = f'rotated/{os.path.basename(img_path)}'
    fig.savefig(path)

    return path

def main(files_in):
    with Pool() as p:
        files_out = p.map(rotate_image, files_in)

    return files_out
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

