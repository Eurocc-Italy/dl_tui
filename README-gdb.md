# Gemello Digitale di Bologna (GDB) custom version

The EuroCC Data Lake infrastructure is set up as follows:

  - An API service running the `dl-api` library is hosted on a cloud VM. This service listens to user requests and calls the TUI library `dl-tui` as needed. The API is also tasked with user authentication via tokens.
  - The `dl-tui` library sets up the environment for HPC jobs, launches them via the queue manager (Slurm), and uploads the results.

## API installation

The `dl-api` library takes care of all Data Lake operations, including upload/download of files. This functionality is not needed for the GDB platform, so it will only be used for launching jobs on HPC. A custom branch was created and should be installed in a virtual environment running on the platform VM:

```bash
git clone -b gdb https://github.com/Eurocc-Italy/dl_api.git
pip install dl_api/datalake_api
```

In the regular EuroCC Data Lake version, the API is running as a user service. The library is installed in the `/opt/dtaas` virtual environment and carries out all operations from the VM user home directory `/home/datalake`. Log files are created at the `/var/log/datalake` path, which should be accessible by the service (NOTE: these logfiles are read for cross-checking job statuses). This is the service file we use for the regular version:

```ini
[Unit]
Description=DTaaS API

[Service]
Type=simple
Restart=always
WorkingDirectory=/home/datalake
ExecStart=/opt/dtaas/bin/swagger_server
Environment=PATH=/opt/dtaas/bin/:/home/datalake/.local/bin:/home/datalake/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin

[Install]
WantedBy=default.target
```

Adjust as necessary for the GDB platform, keeping in mind the `swagger_server` executable should be running on the home directory with write access (only to create temporary files, it deletes everything automatically as soon as the jobs are launched).

## TUI installation

The `dl-tui` library has also been customized for usage with the GDB platform, in particular for the launch of Docker/Singularity containers on HPC. The custom functions have been implemented in the `dlaas/tuilib/gdb.py` file, with some other minor modifications to the other parts of the library for compatibility. Installation of the `dl-tui` library should be carried out as usual via `pip` in a virtual environment, targeting the custom `gdb` branch:

```bash
git clone -b gdb https://github.com/Eurocc-Italy/dl_tui.git
pip install dl_tui
```

This operation should be carried out **both** on the cloud infrastructure hosting the platform/Data Lake API **and** on the HPC cluster, in a virtual environment available in the user home directory, as the setup and launch of the container environment is done via the `dl-tui` library.

Configuration files should be present on the VM running the API as well as on the machine launching the requests to the API (which could also be the same machine). These files, named `config_hpc.json` and `config_server.json` should be stored at the `~/.config/dlaas` path and contain HPC-side and API/server-side informations, respectively. You can take the default templates in `etc/default` for reference.

The `config_hpc.json` mostly contains information that is unused in the GDB version, only the `ip` key should be adjusted, referring to the IP address of the machine running the API, which is where the `dl-tui` interface tries to send the requests. 

The `config_server.json` file should be configured carefully, here are the details:

```json
{
  "user": "username for HPC jobs",
  "host": "address of the HPC cluster",
  "venv_path": "path to the virtual environment on HPC",
  "ssh_key": "path to the HPC SSH key on the API VM",
  "compute_partition": "partition to be used for HPC jobs",
  "upload_partition": "partition to be used for download/upload (with internet access)",
  "account": "HPC account to be used",
  "qos": "Slurm QOS to be used",
  "mail": "mail address to which Slurm job status info should be sent",
  "walltime": "HPC walltime",
  "nodes": "number of nodes for the jobs",
  "ntasks_per_node": "number of cores for the jobs"
}
```

## Token creation

Authentication is carried out via tokens created by the API. In the home directory of the machine running the API there should be a `.env` file containing the following information:

```ini
MONGO_HOST = localhost *
MONGO_PORT = 27017 *
MONGO_DB_NAME = datalake *
MONGO_COLLECTION_NAME = metadata *
S3_BUCKET = DTbo_test_c *
S3_ENDPOINT_URL = https://s3ds.g100st.cineca.it *
PFS_PATH_PREFIX = /leonardo/home/userinternal/lbabetto/DTbo_DTBO-HPC/assets/lidar *
JWT_ISSUER = cineca_or_ifab
JWT_SECRET = eec7f8d5982b9cd41be5a5406bd90164
JWT_LIFETIME_SECONDS = 86400
JWT_ALGORITHM = HS256
```

Entries marked with `*` are not used by the GDB version and can be ignored. The `JWT_...` keys are used for creating and validating authentication tokens:

  - `JWT_ISSUER`: name of the authentication token issuer
  - `JWT_SECRET`: secret key used for generating tokens. Should be set as a random HEX and should NOT be shared with anyone (obviously)
  - `JWT_LIFETIME_SECONDS`: default validity in seconds of the generated tokens
  - `JWT_ALGORITHM`: algorithm used for generating tokens

Tokens can be generated via the `dl_generate_token --user [username] --duration [duration]` executable provided by the API library:

```
(dtaas) [datalake@datalake-as-a-service ~]$ dl_generate_token --user gdb --duration 999999999

Generating authentication token. Please save the token and provide it to the intended user.
Default location for automatic token retrieval: ~/.config/dlaas/api-token.txt

  User: gdb
  Duration: 999999999 seconds
  Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJjaW5lY2Ffb3JfaWZhYiIsImlhdCI6MTczOTUyMzkwMCwiZXhwIjoyNzM5NTIzODk5LCJzdWIiOiJnZGIifQ.WtQoMHFvPdvvhkl3OWZd2VUxj7zgiepAL6vT9VmCIXQ
```

A second security mechanism is in place, in which only tokens generated by the same machine running the API are valid for use with that API. Even if the JWT_SECRET is leaked, it cannot be used on its own to generate valid tokens. But, if you want to generate a valid token you must do so from the same machine in which the API service is going to run.

If the GDB platform already has other authentication protocols in place this step can be simplified and the token can be saved with a very long expiration date in the `~/.config/dlaas/api-token.txt` file of the machine performing the API calls via the TUI.

## Usage

Before being able to use the Data Lake API/TUI, the following prerequisites must be met:

  - The API server (`swagger_server`) should be up and running on a VM with SSH access to the HPC cluster. The SSH key should be stored at the path indicated in the `ssh_key` key in the `config_server.json` config file.
  - The TUI should be installed: i) on the machine running the API; ii) on the machine making the requests to the API; iii) on the HPC machine at the path indicated at the `venv_path` in `config_server.json`
  - A valid authentication token generated with the `dl_generate_token` utility should be stored at the `~/.config/dlaas/api-token.txt` path on the machine making API requests

From here, container launch requests can be made to the API via the TUI using the following command:

```bash
dl_tui --query --query_file query.txt --container_url docker://ghcr.io/lbabetto/gdb-hpc-integration:main --input_json input.json --output_json output.json
```

Where:

  - `--query_file` is a plain text file which should contain the SQL query for looking up files in the original Data Lake implementation. Since input files are already provided by the platform, this file can be a dummy containing something like `SELECT * FROM metadata WHERE field = none`, since this functionality is bypassed for the GDB version.
  - `--container_url` is the URL from which to download the Docker/Singularity container. To provide a pre-built Singularity image, use `--container_path=/path/to/container.sif` instead
  - `--input_json` is a JSON containing the `"filename": "pre-signed URL"` key-value pairs for the files to be **downloaded** to HPC from the GDB S3
  - `--output_json` is a JSON containing the `"filename": "pre-signed URL"` key-value pairs for the files to be **uploaded** to the GDB S3 from HPC

The containers will have access to a read-only bound `/input` folder where all the files indicated in `--input_json` are downloaded, and a `/output` folder where all results indicated in `--output_json` should be stored for upload.