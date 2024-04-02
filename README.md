# dlaas-tui

This is the user guide for Cineca's Data Lake as a Service Text User Interface (`dlaas-tui`) Python library. This TUI is used to run queries and processing scripts on the Data Lake files, as well as interacting with the API server administering the Data Lake infrastructure.

## Data Lake as a Service

The Data Lake as a Service provides a fast deploy system for the core components of the Data Lake and its interface. It is designed to deliver a customizable infrastructure according to the needs of the requesting user and integrate it with High Performance Computing (HPC) systems, enabling high-performance analytics on extensive datasets, increasing throughput and/or reducing time to solution. 

The service provides for two hardware endpoints:

  - a Cloud-based VM running a MongoDB database instance and the API server. The database contains the metadata for all the files in the Data Lake, including the path to the corresponding file in the HPC parallel filesystem, while the API server is tasked with orchestrating the interaction between the user and the Data Lake components;
  - an HPC cluster which contains the actual files, stored in dual parallel filesystem/object storage (S3) mode. The HPC part runs the processing script sent by the user on the files matching the query.

The software stack is composed of three main components:

  - A [repository](https://gitlab.hpc.cineca.it/igentile/dtaas-digitaltwinasaservice) containing the Ansible deployment scripts for setting up the infrastructure;
  - An [API](https://gitlab.hpc.cineca.it/igentile/dtaas_test_api) server, which accepts the user requests and carries out operations on the Data Lake (file upload/download, querying, etc.);
  - The TUI (this library), which is utilized by the end users to interface with the API, as well as by the API itself to interface with the HPC system.

## Text User Interface (`dlaas-tui`)

This library consists of three executables: `dl_tui`, `dl_tui_hpc` and `dl_tui_server`. 

  * `dl_tui` is used to launch commands to the service API for interacting with the Data Lake.
  * `dl_tui_hpc` is intended to be run on the machine with direct access to the files of the Data Lake (HPC) and runs the querying and processing.
  * `dl_tui_server` version is intended to be run on the VM. Its purpose is to parse the user input (query and processing script) and launch a Slurm job on HPC with the user request.

The `dl_tui` executable is intended to be used by the users themselves (see the [API Wrapper](#api-interface-dl_tui) section), while the `dl_tui_hpc` and `dl_tui_hpc` executables are intended to be used by the API server.

### Code structure

The library code is found in the `dlaas` folder, with the following structure:

```
dlaas
├── __init__.py
├── bin
│   ├── __init__.py
│   ├── dl_tui.py
│   ├── dl_tui_hpc.py
│   └── dl_tui_server.py
├── etc/default
│   ├── config_hpc.json
│   └── config_server.json
└── tuilib
    ├── __init__.py
    ├── api.py
    ├── hpc.py
    ├── common.py
    └── server.py
```

The `bin` directory contains the main executables, `dl_tui`, `dl_tui_hpc`, and `dl_tui_server`. 

The `tuilib` folder contains the various functions used by the main executables.

The `etc/default` folder contains some default settings, and can be taken as a template for building custom configurations.

### Installation

We highly recommend installing the software in a custom Python virtual environment. You can set up a virtual environment with third-party tools such as [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html) or with the built-in [venv](https://docs.python.org/3/library/venv.html) module.

After having set up and activated your virtual environment, follow these steps to install the DTaaS TUI:
  
  ```bash
  git clone https://gitlab.hpc.cineca.it/lbabetto/dlaas-tui  
  pip install dlaas-tui/
  ```

### API interface (`dl_tui`)

It is possible to use the `dl_tui` executable to interact with the API server on the VM for uploading, downloading, replacing, and updating files, as well as launching queries for processing data and browsing the contents of the Data Lake. For running high-performance analytics on the Data Lake files, see the [corresponding](#high-performance-analytics) section.

The general syntax for command-line calls is:

```
dl_tui action option1=value1 option2=value2 ...
```

The API interface can be called via the `dl_tui` executable, with one of the following _actions_:

  * `upload`
  * `replace`
  * `update`
  * `download`
  * `delete`
  * `query`
  * `browse`

The IP address of the API server will be taken by the `config_hpc.json` configuration file (see the [configuration](#configuration) section for more details). Alternatively, it is possible to overwrite the default via the `ip=...` _option_.

A valid authentication token is required. If saved in the `~/.config/dlaas/api-token` file, it will automatically be read by the executable. Otherwise, the token can be sent directly via the `token=...` _option_.

The `upload` and `replace` _actions_ require the following additional (mandatory) _options_:

  * `file=...`: path to the file to be uploaded to the Data Lake
  * `json_data=...`: path to the .json file containing the metadata of the file to be uploaded to the Data Lake

The `update` _action_ also requires the `file=...` and `json_data=...` (mandatory) _options_, but in this case the `file=...` should be the S3 key corresponding to the file (_i.e._, the filename).

The `download` and `delete` _actions_ require a `file=...` (mandatory) _option_, which similarly to the `update` _action_ should be the S3 key corresponding to the file to be downloaded/deleted.

The `query` _action_ requires the following additional _options_:

  * `query_file=...` (mandatory): path to the text file containing the SQL query to be ran on the Data Lake.
  * `python_file=...` (optional): path to the Python file containing the processing to be ran on the files matching the query.

If no Python file is provided, the job will match the files of the query and copy them to the results archive for download.

The `browse` _action_ allows for an additional _option_ `filter=...` which accepts an SQL-like query string for listing the requested files, removing the `SELECT * FROM metadata WHERE` part of the query itself and only leaving the filters. For example, `SELECT * FROM metadata WHERE category = dog OR category = cat` becomes `filter="category = dog OR category = cat"`.

Example commands:

  * Upload: `dl_tui upload file=/path/to/file.csv json_data=/path/to/metadata.json`
  * Replace: `dl_tui replace file=/path/to/updated/file.csv json_data=/path/to/updated_metadata.json`
  * Update: `dl_tui update file=file.csv json_data=/path/to/updated_metadata.json`
  * Download: `dl_tui download file=file.csv`
  * Query: `dl_tui query query_file=/path/to/query.txt python_file=/path/to/script.py`
  * Browse: `dl_tui browse filter="category = dog"`

### High-performance analytics

The library enables high-performance analytics on the Data Lake files via the `dl_tui query` action. The TUI will fetch the list of files matching the given SQL query and will run the user-provided Python script on these files. The results will be uploaded to the Data Lake and made available for download.

The Python script must satisfy the following requirements:

  * It must feature a `main` function, which will be **all** that is actually run on HPC (_i.e._, any piece of code not explicitly present in the `main` function will not be executed). Helper functions can be declared anywhere, but must be explicitly called in `main`;
  * The `main` function must accept a list of file paths as input, which will be populated with the matches of the SQL query;
  * The `main` function should return a list of file paths as output, corresponding to the files which the user wants to save from the analysis. The interface will then take this list of paths, save the corresponding files (generated _in situ_ on HPC) in an archive which is uploaded to the S3 bucket and made available to the user for download via the API.

Below is an example of a valid Python script to be passed to the interface, with a `main` function taking a list of paths as input and returning a list of paths as output. 

The script uses the [scikit-image](https://scikit-image.org/) library to rotate the images given in input and save them alongside the original, using [matplotlib](https://matplotlib.org/) to generate the comparison "graphs" and [imageio](https://imageio.readthedocs.io/en/stable/) to open the actual image files. The comparison images are saved in a temporary folder (`rotated`) whose contents are returned by the main function in form of file paths. The script also uses the Python multiprocessing libary to run the job in parallel, exploiting the HPC performance for the analysis.

```python
import imageio as iio
import skimage as ski
import matplotlib.pyplot as plt
import os, sys
from multiprocessing import Pool

def rotate_image(img_path):  # "worker" function, expecting a file path as input
    os.makedirs('rotated', exist_ok=True)  # Create temporary folder if doesn't exist

    image = iio.imread(uri=img_path)  # Open file with image
    rotated = ski.transform.rotate(image=image, angle=45)  # Rotate image

    fig, ax = plt.subplots(1,2)  # Initialize comparison plot
    ax[0].imshow(image)  # Print original image to the left
    ax[1].imshow(rotated)   # Print rotated image to the right

    path = f'rotated/{os.path.basename(img_path)}'  # Generate path name using original image name
    fig.savefig(path)  # Save image

    return path  # Return the path of the newly saved image

def main(files_in):  # main function, expecting a list of file paths as input
    with Pool() as p:  # Use all available cores
        files_out = p.map(rotate_image, files_in)  # run the rotate_image function on all files in parallel

    return files_out  # Return list of file paths

print("Done!")  # NOTE: This line will not be executed, as it is not in the `main` function!
```

### Input for `dl_tui_hpc`/`dl_tui_server` executables (API only)

The input parameter of the `dl_tui_hpc` and `dl_tui_server` executables should be the path to a properly-formatted JSON document, whose content should be the following:

  - `id`: unique identifier characterizing the run. We recommend using the [UUID](https://docs.python.org/3/library/uuid.html) module to generate it, producing a UUID.hex string
  - `sql_query`: SQL query to be run on the MongoDB database containing the metadata. Since the Data Lake is by definition a non-relational database, and the "return" of the query is the file itself, most queries will be of the type `SELECT * FROM metadata WHERE [...]`.
  - `script_path` (optional): path to a Python script to analyse the files matching the query. This script must feature a `main` function taking as input a list of file paths, which will be populated by the interface with the files matching the query, and returning a list of file paths as output, which will be saved in a compressed archive and made available to the user.
  - `config_hpc` (optional): a dictionary containing options for hpc-side configuration
  - `config_server` (optional): a dictionary containing options for server-side configuration

After you prepared the JSON file (for example, called `input.json`), the program can be called as such:

```
dl_tui_<hpc/server> input.json
```

If no script is provided, the program will simply return the files matching the query.

### Configuration

The library first loads the default options written in the JSON files located in the `dlaas/etc/default` folder (which can be taken as a template to understand the kind of options which can be configured).

If you wish to overwrite these defaults and customise your configuration, it is recommended to save a personalised version of the `config_hpc.json` and `config_server.json` files in the `~/.config/dlaas` folder. The options indicated here will take precedence over the defaults. Missing keys will be left at the default values.

If you wish to send custom configuration keys on the fly, it is also possible to pass configuration options to the `dl_tui_<hpc/server>` executables via the `config_hpc` and `config_server` keys in the input JSON file, also in JSON format. These will take precedence over both defaults and what is found in `~/.config/dlaas/config_<hpc/server>.json`.

For the hpc version, the configurable options are relative to the server VM:

  * `user`: the user name in the MongoDB server
  * `password`: the password of the MongoDB server
  * `ip`: network address of the machine running the MongoDB server
  * `port`: the port to access the MongoDB server
  * `database`: the name of the MongoDB database
  * `collection`: the name of the MongoDB collection within the database
  * `s3_endpoint_url`: URL at which the S3 bucket can be found
  * `s3_bucket`: name of the S3 bucket storing the Data Lake files 
  * `pfs_prefix_path`: path at which the Data Lake files are stored on the parallel filesystem

For the server version, the configurable options are relative to the HPC system:

  * `user`: username of the HPC account
  * `host`: address of the HPC login node
  * `venv_path`: path of the virtual environment in which the library is installed
  * `ssh_key`: path to the SSH key used for authentication on the HPC login node
  * `compute_partition`: SLURM partition for running the HPC job
  * `upload_partition`: SLURM partition for uploading the files to the S3 bucket
  * `account`: SLURM account for the HPC job
  * `qos`: SLURM QoS for the HPC job
  * `mail`: email address to which the notifications for job start/end are sent
  * `walltime`: maximum walltime for the HPC job
  * `nodes`: number of nodes requested for the HPC job
  * `ntasks_per_node`: number of CPU cores per node requested for the HPC job

> **NOTE:**
> The `config_<hpc/server>.json` file names reflect the executables which need them, not the system to which the information within pertains. *e.g.*, the `config_server.json` mostly contains HPC-related information, but is used by the `dl_tui_server` executable which is supposed to run on the server VM, hence the name.