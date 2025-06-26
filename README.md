# dl_tui

This is the user guide for EuroCC-Italy's Data Lake Ready to Use Text User Interface (`dl_tui`) Python library. This TUI is used to run queries and high-performance analytics on the Data Lake files, as well as interacting with the API server administering the Data Lake infrastructure.

This repository is part of the EuroCC-Italy Data Lake Ready to Use codebase. The repositories in this codebase are:

- [`dl_tui`](https://github.com/Eurocc-Italy/dl_tui): (Text User Interface)
- [`dl_api`](https://github.com/Eurocc-Italy/dl_api): (API)
- [`dl_deploy`](https://github.com/Eurocc-Italy/dl_deploy): (Ansible playbook for self-deploy)

The User Guide can be found in PDF form at this [link](https://github.com/Eurocc-Italy/dl_deploy/blob/main/Data_Lake_Ready_To_Use_V1.0.pdf).

## Data Lake Ready to Use

The Data Lake Ready to Use provides a fast deploy system for the core components of the Data Lake and its interface. It is designed to deliver a customizable infrastructure according to the needs of the requesting user and integrate it with High Performance Computing (HPC) systems, enabling high-performance analytics on extensive datasets, increasing throughput and/or reducing time to solution.

The service provides for two hardware endpoints:

- a Cloud-based VM running a MongoDB database instance and the API server. The database contains the metadata for all the files in the Data Lake, including the path to the corresponding file in the HPC parallel filesystem, while the API server is tasked with orchestrating the interaction between the user and the Data Lake components;
- an HPC cluster which contains the actual files, stored in dual parallel filesystem/object storage (S3) mode. The HPC part runs the processing script sent by the user on the files matching the query.

The software stack is composed of three main components:

- A [repository](https://github.com/Eurocc-Italy/dl_deploy) containing the Ansible deployment scripts for setting up the infrastructure;
- An [API](https://github.com/Eurocc-Italy/dl_api) server, which accepts the user requests and carries out operations on the Data Lake (file upload/download, querying, etc.);
- The TUI (this library), which is utilized by the end users to interface with the API, as well as by the API itself to interface with the HPC system.

## Text User Interface (`dl_tui`)

This library consists of three executables: `dl_tui`, `dl_tui_hpc` and `dl_tui_server`.

- `dl_tui` is used to launch commands to the service API for interacting with the Data Lake.
- `dl_tui_hpc` is intended to be run on the machine with direct access to the files of the Data Lake (HPC) and runs the querying and processing.
- `dl_tui_server` version is intended to be run on the VM. Its purpose is to parse the user input (query and processing script) and launch a Slurm job on HPC with the user request.

The `dl_tui` executable is intended to be used by the users themselves (see the [API Wrapper](#api-interface-dl_tui) section), while the `dl_tui_hpc` and `dl_tui_hpc` executables are intended to be used by the API server.

### Code structure

The library code is found in the `dlaas` folder, with the following structure:

```Text
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
  
  ```shell
  git clone https://github.com/Eurocc-Italy/dl_tui  
  pip install dl_tui/
  ```

### API interface (`dl_tui`)

It is possible to use the `dl_tui` executable to interact with the API server on the VM for uploading, downloading, replacing, and updating files, as well as launching queries for processing data and browsing the contents of the Data Lake.

The general syntax for command-line calls is:

```shell
dl_tui --action --option1=value1 --option2=value2 ...
```

The API interface can be called via the `dl_tui` executable. The following _actions_ are supported:

- `--upload`
- `--replace`
- `--update`
- `--download`
- `--delete`
- `--query`
- `--browse`
- `--job_status`

The IP address of the API server will be taken by the `config_hpc.json` configuration file (see the [configuration](#configuration) section for more details). Alternatively, it is possible to overwrite the default via the `--ip=...` _option_.

A valid authentication token is required. If saved in the `~/.config/dlaas/api-token.txt` file, it will automatically be read by the executable. Otherwise, the token can be sent directly via the `--token=...` _option_.

### Basic I/O operations

#### Upload

To upload files to the Data Lake, use the `--upload` _action_. The following _options_ are available:

- `--file=...`: path to the file to be uploaded to the Data Lake
- `--metadata=...`: path to the .json file containing the metadata of the file to be uploaded to the Data Lake

Example:

```shell
dl_tui --upload --file=/path/to/file.csv --metadata=/path/to/metadata.json
```

>NOTE: existing files cannot be replaced with this action. To replace an existing file or its metadata, use the `--replace` or `--update` actions.

#### Download

To download files from the Data Lake, use the `--download` _action_. The following _options_ are available:

- `--key=...`: S3 key (equal to the filename) corresponding to the file to be downloaded

Example:

```shell
dl_tui --download --key=file.csv
```

#### Delete

To delete files from the Data Lake, use the `--delete` _action_. The following _options_ are available:

- `--key=...`: S3 key (equal to the filename) corresponding to the file to be deleted

Example:

```shell
dl_tui --delete --key=file.csv
```

#### Replace

To replace a Data Lake file and its metadata, use the `--replace` _action_. The following _options_ are available:

- `--file=...`: path to the file to be replaced in the Data Lake
- `--metadata=...`: path to the .json file containing the metadata of the file to be replaced in the Data Lake

>NOTE: only existing files can be replaced. To upload a new file, use the `--upload` action.

Example:

```shell
dl_tui --replace --file=/path/to/updated/file.csv --metadata=/path/to/updated_metadata.json
```

#### Update

To only update the metadata of a Data Lake file (without replacing the file itself), use the `--update` _action_. The following _options_ are available:

- `--key=...`: S3 key of the file which needs to be updated
- `--metadata=...`: path to the .json file containing the metadata of the file to be updated in the Data Lake

Example:

```shell
dl_tui --update --key=file.csv --metadata=/path/to/updated_metadata.json
```

### High-performance analytics

The library enables high-performance analytics on the Data Lake files via the `--query` action. The TUI will fetch the list of files matching the given SQL query, run the analysis on these files and upload the results back to the Data Lake. The following modes are currently supported:

- Python scripts
- Docker/Singularity containers

Analytics are run on Data Lake files using a query system. Users can select the files to be analyzed by writing a SQL query in a text file and providing it via the `--query_file` _option_.

#### Simple query

If only the query file is provided, the corresponding files are zipped to an archive and uploaded to the Data Lake, ready for download. A job ID will be provided after the command launch. This will be the unique identifier for the query, and results will be provided in a .zip file named `results_<JOB_ID>.zip`.

Example:

```shell
dl_tui --query --query_file=/path/to/query.txt
Successfully launched query SELECT * FROM datalake WHERE field = value

Job ID: ddb66778cd8649f599498e5334126f9d
```

#### Python scripts

It is possible to provide a Python script for analysis, and have the Data Lake run the script on the files matching the query. The path to the Python file should be provided using the `--python_file` _option_. The script needs to satisfy the following requirements:

- It must feature a `main` function, which will be **all** that is actually run on HPC (_i.e._, any piece of code not explicitly present in the `main` function will not be executed). Helper functions can be declared anywhere, but must be explicitly called in `main`
- The `main` function must accept a list of file paths as input, which will be populated automatically with the matches of the SQL query
- The `main` function should return a list of file paths as output, corresponding to the files which the user wants to save from the analysis. The interface will then take this list of paths, save the corresponding files (generated _in situ_ on HPC) into an archive which is then uploaded to the S3 bucket and made available to the user for download via the API

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

To launch the analysis, use the following command:

```shell
$ dl_tui --query --query_file=/path/to/query.txt --python_file=/path/to/script.py
Successfully launched analysis script /path/to/script.py on query SELECT * FROM datalake WHERE field = value

Job ID: a2b8355051940f6cf09ff225d8340389
```

#### Docker/Singularity containers

It is also possible to run a Docker/Singularity container to analyze the files matching the query. Users can either provide the path to the container image via the `--container_path` _option_, or alternatively provide the URL of the image to be used via the `--container_url` _option_. By default, containers are launched in "run" mode (equivalent to `docker/singularity run image.sif`). It is possible to provide a specific executable to be used via the `--exec_command` _option_; in this case the container is launched in "exec" mode (equivalent to `docker/singularity exec /path/to/executable image.sif`).

Input files will be automatically provided by the Data Lake API to the container executable based on the query matches. An `/input` folder will be bound to the container corresponding to the folder on the parallel filesystem where Data Lake files are stored (in read-only mode).

The executable should expect input file paths as CLI arguments (equivalent to `docker/singularity run /input/file1.png /input/file2.png /input/file3.png ...`)

The container should save all results that should be uploaded to the Data Lake to the `/output` folder, which will automatically be created and bound by the Data Lake infrastructure at runtime

### Extra utilities

#### Browse files

The `--browse` _action_ allows users to browse the Data Lake content. The _option_ `--filter=...` is available, which accepts an SQL-like query string for listing the requested files, removing the `SELECT * FROM metadata WHERE` part of the query itself and only leaving the filters. For example, `SELECT * FROM metadata WHERE category = dog OR category = cat` becomes `filter="category = dog OR category = cat"`.

Example:

```shell
$ dl_tui --browse --filter="category = parquet"
Filter: category = parquet
Files:
  - file_1.parquet
  - file_2.parquet
  - file_3.parquet
```

#### Check job status

The `--job_status` _action_ allows users to check the status of jobs running on HPC.

```shell
$ dl_tui --job_status
                            JOB ID  SLURM JOB   STATUS REASON
  42684ce4c6d2440b8f9ad6647581a52d   14673377  PENDING Dependency
```

### Configuration

The library first loads the default options written in the JSON files located in the `dlaas/etc/default` folder (which can be taken as a template to understand the kind of options which can be configured).

If you wish to overwrite these defaults and customise your configuration, it is recommended to save a personalised version of the `config_hpc.json` and `config_server.json` files in the `~/.config/dlaas` folder. The options indicated here will take precedence over the defaults. Missing keys will be left at the default values.

If you wish to send custom configuration keys on the fly, it is also possible to pass configuration options to the `dl_tui_<hpc/server>` executables via the `config_hpc` and `config_server` keys in the input JSON file, also in JSON format. These will take precedence over both defaults and what is found in `~/.config/dlaas/config_<hpc/server>.json`.

For the hpc version, the configurable options are the following:

- `user`: the user name in the MongoDB server
- `password`: the password of the MongoDB server
- `ip`: network address of the machine running the MongoDB server
- `port`: the port to access the MongoDB server
- `database`: the name of the MongoDB database
- `collection`: the name of the MongoDB collection within the database
- `s3_endpoint_url`: URL at which the S3 bucket can be found
- `s3_bucket`: name of the S3 bucket storing the Data Lake files
- `pfs_prefix_path`: path at which the Data Lake files are stored on the parallel filesystem
- `modules`: list of system modules to be loaded on HPC

For the server version, the configurable options are the following:

- `user`: username of the HPC account
- `host`: address of the HPC login node
- `venv_path`: path of the virtual environment in which the library is installed
- `ssh_key`: path to the SSH key used for authentication on the HPC login node
- `compute_partition`: SLURM partition for running the HPC job
- `upload_partition`: SLURM partition for uploading the files to the S3 bucket
- `account`: SLURM account for the HPC job
- `qos`: SLURM QoS for the HPC job
- `mail`: email address to which the notifications for job start/end are sent
- `walltime`: maximum walltime for the HPC job
- `nodes`: number of nodes requested for the HPC job (`srun` will automatically use this value)
- `tasks_per_node`: number of processes per node to use for the HPC job (`srun` will automatically use this value)
- `cpus_per_node`: number of CPU cores assigned to each process for the HPC job
- `gpus`: number of GPUs requested for the HPC job

> **NOTE:**
> The `config_<hpc/server>.json` file names reflect the executables which need them, not the system to which the information within pertains. _e.g._, the `config_server.json` mostly contains HPC-related information, but is used by the `dl_tui_server` executable which is supposed to run on the server VM, hence the name.

### Input for `dl_tui_hpc`/`dl_tui_server` executables (API only)

The input parameter of the `dl_tui_hpc` and `dl_tui_server` executables should be the path to a properly-formatted JSON document, whose content should be the following:

- `id`: unique identifier characterizing the run. We recommend using the [UUID](https://docs.python.org/3/library/uuid.html) module to generate it, producing a UUID.hex string
- `sql_query`: SQL query to be run on the MongoDB database containing the metadata. Since the Data Lake is by definition a non-relational database, and the "return" of the query is the file itself, most queries will be of the type `SELECT * FROM metadata WHERE [...]`
- `script_path` (optional): path to a Python script to analyse the files matching the query. This script must feature a `main` function taking as input a list of file paths, which will be populated by the interface with the files matching the query, and returning a list of file paths as output, which will be saved in a compressed archive and made available to the user
- `container_path` (optional): path to a Docker/Singularity container to analyse the files matching the query. The container executable should expect a list of file paths as input, and should save all relevant output to the `/output` folder
- `container_url` (optional): URL to a Docker/Singularity container to analyse the files matching the query. The container executable should expect a list of file paths as input, and should save all relevant output to the `/output` folder
- `exec_command` (optional): command to be run in the Docker/Singularity container in `exec` mode
- `config_hpc` (optional): a dictionary containing options for hpc-side configuration
- `config_server` (optional): a dictionary containing options for server-side configuration

After you prepared the JSON file (for example, called `input.json`), the program can be called as such:

```shell
dl_tui_<hpc/server> input.json
```

If no script is provided, the program will simply return the files matching the query.
