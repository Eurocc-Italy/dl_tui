# DTaaS-TUI

Query/processing text user interface for the DTaaS project.

Consists of two executables, `dtaas_tui_client` and `dtaas_tui_server`, which take as argument a JSON-formatted dictionary containing the following keys:

 - a unique ID (we recommend using the [UUID](https://docs.python.org/3/library/uuid.html) module to generate it)
 - a SQL query to be run on the MongoDB database containing the metadata 
 - the content of a Python script to analyse the files matching the query

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

If the `dtaas_tui_client` version is called, the interface will run locally (still accessing the remote MongoDB server), therefore files should be available on the local machine.

If the `dtaas_tui_server` version is called, the interface will launch a job on HPC, where the processing will run on the compute nodes, accessing the files on the parallel filesystem.

If no script is provided, the results will simply be the files corresponding to the query.

## Passing JSON-formatted input

To be correctly parsed by the interface, the input argument should be a properly-formatted JSON document, where all special characters have been correctly escaped. This is to ensure that, for example, the shell does not strip quotes or parse wildcards.

Since the client version runs locally, special characters only need to be escaped once. In the client version, because of the way it works (two shell "passages" are involved), escape characters need to be escaped themselves, otherwise they get "lost in translation".

The library provides a `sanitize_string("client/server", string)`, which needs the user to specify whether they want a string intended for the client/server version, and the string to "sanitize", returning the appropriately-formatted version.

Example of a correct call of the `dtaas_tui_client` program, which only runs the query:

```
dtaas_tui_client {\"ID\": \"727D2CAB7D0944E9830A9B413A6A33BF\", \"query\": \"SELECT \* FROM metadata WHERE capture_date = \'2013-11-14\'\"}
```

Example of a correct call of the `dtaas_tui_server` program, which runs the following Python script:

```
import imageio as iio
import skimage as ski
import matplotlib.pyplot as plt
import os, sys
from multiprocessing import Pool

def rotate_image(img_path):
    os.makedirs("rotated", exist_ok=True)

    image = iio.imread(uri=img_path)
    rotated = ski.transform.rotate(image=image, angle=45)

    fig, ax = plt.subplots(1,2)
    ax[0].imshow(image)
    ax[1].imshow(rotated)

    path = f"rotated/{os.path.basename(img_path)}"
    fig.savefig(path)

    return path

def main(files_in):
    with Pool() as p:
        files_out = p.map(rotate_image, files_in)

    return files_out
```

Note the extra backslashes:

```
dtaas_tui_server {\\\"ID\\\": \\\"727D2CAB7D0944E9830A9B413A6A33BF\\\", \\\"query\\\": \\\"SELECT \\* FROM metadata WHERE capture_date = \\\'2013-11-14\\\'\\\", \\\"script\\\": \\\"import imageio as iio\\\\nimport skimage as ski\\\\nimport matplotlib.pyplot as plt\\\\nimport os, sys\\\\nfrom multiprocessing import Pool\\\\n\\\\ndef rotate_image\(img_path\):\\\\n    os.makedirs\(\\\'rotated\\\', exist_ok=True\)\\\\n\\\\n    image = iio.imread\(uri=img_path\)\\\\n    rotated = ski.transform.rotate\(image=image, angle=45\)\\\\n\\\\n    fig, ax = plt.subplots\(1,2\)\\\\n    ax[0].imshow\(image\)\\\\n    ax[1].imshow\(rotated\)\\\\n\\\\n    path = f\\\'rotated/{os.path.basename\(img_path\)}\\\'\\\\n    fig.savefig\(path\)\\\\n\\\\n    return path\\\\n\\\\ndef main\(files_in\):\\\\n    with Pool\(\) as p:\\\\n        files_out = p.map\(rotate_image, files_in\)\\\\n\\\\n    return files_out\\\"}
```