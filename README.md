# dtaas-tui

This is the user guide for the Digital Twin as a Service Text User Interface (`dtaas-tui`) Python library. The TUI is used to query the data lake and run processing scripts on the files matching the query.

The service is composed of: 

  - a Cloud-based infrastructure with a VM running a MongoDB database instance. This database contains the metadata for all the files in the data lake / digital twin, including the path to the actual corresponding file in the HPC parallel filesystem.
  - a HPC cluster which contains the actual files, stored in dual parallel filesystem/S3 mode. The HPC part runs the processing script sent by the user on the files matching the query.

The library consists of two main executables, `dtaas_tui_client` and `dtaas_tui_server`. 

The `client` version is intended to be run on the machine with direct access to the files of the data lake and runs the actual querying and processing.

The `server` version is intended to be run on a cloud machine with access to an HPC infrastructure running Slurm. Its purpose is to parse the user input (query and processing script) and launch a Slurm job on HPC which calls the client version.

A third executable, `dtaas_api` is provided and is used to launch commands to the service API to interact with the Data Lake portion of the service.

## Code structure

The library code is found in the `dtaas` folder, with the following structure:

```
dtaas
├── __init__.py
├── bin
│   ├── __init__.py
│   ├── dtaas_api.py
│   ├── dtaas_tui_client.py
│   └── dtaas_tui_server.py
├── etc/default
│   ├── config_client.json
│   └── config_server.json
└── tuilib
    ├── __init__.py
    ├── api.py
    ├── client.py
    ├── common.py
    └── server.py
```

The `bin` directory contains the main executables, `dtaas_tui_client`, `dtaas_tui_server`, and `dtaas_api`. These executables utilize the various functions from the files present in the `tuilib` folder, according to the user request.

The `etc/default` folder contains the default settings for the executables, and can be taken as a template for building custom configurations.

## Installation

We highly recommend installing the software in a custom Python virtual environment. You can set up a virtual environment with third-party tools such as [Anaconda](https://docs.anaconda.com/free/anaconda/install/index.html) or with the built-in [venv](https://docs.python.org/3/library/venv.html) module.

After having set up and activated your virtual environment, follow these steps to install the DTaaS TUI:
  
  ```bash
  git clone https://gitlab.hpc.cineca.it/lbabetto/dtaas-tui  
  pip install dtaas-tui/
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

The Python script must have a `main` function, which will be what is actually run by the executable. This function should take a list as input, which will be populated by the interface with the paths of the files matching the SQL query. The function should also return a list as output, which contains the paths of the files which the user wants to save from the analysis. The interface will then take this list of paths, save the corresponding files (generated _in situ_ on HPC) in an archive which is uploaded to S3 and made available to the user for download via the API.

This is an example of a valid Python script to be passed to the interface, with a `main` function taking a list of paths as input and returning a list of paths as output. 

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
```

## Configuration

The library first loads the default options written the JSON files located in the library's `dtaas/etc/default` folder (which can be taken as a template to understand the kind of options which can be configured).

If you wish to overwrite these defaults and customise your configuration, it is recommended to save a personalised version of the `client_config.json` and `server_config.json` files in the `~/.config/dtaas-tui` folder. The options indicated here will take precedence over the defaults. Missing keys will be left at the default values.

If you wish to send a custom configurazion on the fly, it is also possible to pass configuration options via the `config_client` and `config_server` keys in the input JSON file, also in JSON format. These will take precedence over both defaults and what is found in `~/.config/dtaas-tui/config_<client/server>.json`.

For the client version, the configurable options are:

  * `user`: the user name in the MongoDB server
  * `password`: the password of the MongoDB server
  * `ip`: network address of the machine running the MongoDB server
  * `port`: the port to access the MongoDB server
  * `database`: the name of the MongoDB database
  * `collection`: the name of the MongoDB collection within the database
  * `s3_endpoint_url`: URL at which the S3 bucket can be found
  * `s3_bucket`: name of the S3 bucket storing the data lake files 
  * `pfs_prefix_path`: path at which the data lake files are stored on the parallel filesystem

For the server version, the configurable options are:

  * `user`: username of the HPC account
  * `host`: address of the HPC login node
  * `venv_path`: path of the virtual environment in which the library is installed
  * `ssh_key`: path to the SSH key used for authentication on the HPC login node
  * `compute_partition`: SLURM partition for running the HPC job
  * `upload_partition`: SLURM partition for uploading the files to the S3 bucket
  * `account`: SLURM account for the HPC job
  * `qos`: SLURM QoS for the HPC job
  * `mail`: email address to which the notifications for job start/end are sent
  * `walltime`: maximum walltime for HPC job
  * `nodes`: number of nodes requested for HPC job
  * `ntasks_per_node`: number of CPU cores per node requested for the HPC job

## API Wrapper

It is also possible to interact with the API server on the VM hosting the metadata database for uploading, downloading, replacing, and updating files, as well as launching queries for processing data.

The wrapper can be called via a third executable, `dtaas_api`, with one of the following actions:

  * `upload`
  * `replace`
  * `update`
  * `download`
  * `delete`
  * `query`

The IP address of the API server will be taken by the `client_config.json` configuration file. Alternatively, it is possible to overwrite the default via the `ip=...` option.

A valid authentication token is required. If saved in the `~/.config/dtaas-tui/api-token` file, it will automatically be read by the wrapper. Otherwise, the token can be sent directly via the `token=...` option.

The `upload` and `replace` actions require the following additional options:

  * `file=...`: path to the file to be uploaded to the Data Lake
  * `json_data=...`: path to the .json file containing the metadata of the file to be uploaded to the Data Lake

The `update` action also requires the `file=...` and `json_data=...` options, but in this case the `file=...` should be the S3 key corresponding to the file (i.e., the filename).

The `download` and `delete` actions require a `file=...` option, which similarly to the `update` action should be the S3 key corresponding to the file to be downloaded/deleted.

The `query` action requires the following additional options:

  * `query_file=...`: path to the text file containing the SQL query to be ran on the Data Lake.
  * `python_file=...`: path to the Python file containing the processing to be ran on the files matching the query.

Example commands:

  * Upload: `dtaas_api upload file=/path/to/file.csv json_data=/path/to/metadata.json`
  * Replace: `dtaas_api replace file=/path/to/updated/file.csv json_data=/path/to/updated_metadata.json`
  * Update: `dtaas_api update file=file.csv json_data=/path/to/updated_metadata.json`
  * Download: `dtaas_api download file=file.csv`
  * Query: `dtaas_api upload query_file=/path/to/query.txt python_file=/path/to/script.py`