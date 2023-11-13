# User guide

To be correctly parsed, the input argument of the executable should be a properly-formatted JSON document, where all special characters have been escaped. This is to ensure that, for example, the shell does not strip quotes or parse wildcards.

The content of the JSON document should be the following:

  - `ID`: unique identifier characterizing the run. We recommend using the [UUID](https://docs.python.org/3/library/uuid.html) module to generate it, producing a UUID.hex string
  - `query`: SQL query to be run on the MongoDB database containing the metadata. Since the data lake is by definition a non-relational database, and the "return" of the query is the file itself, most queries will be of the type `SELECT * FROM metadata WHERE [...]`.
  - `script` (optional): content of the Python script to analyse the files matching the query. This script must feature a `main` function taking as input a list of file paths, which will be populated by the interface with the files matching the query, and returning a list of paths as output, which will be saved in a compressed archive and made available to the user. For more information, see the [User Guide](user-guide). If no script is provided, the program will simply return the files matching the query.

:::{admonition} Warning!
:class: warning 
Unfortunately, double quotes are currently forbidden both in the SQL query and the user script. If you must use quotes, please use single quotes or the program will not work!
:::

Since the client version runs locally, special characters only need to be escaped once. In the client version, because of the way it works (two shell "passages" are involved), escape characters need to be escaped themselves, otherwise they get "lost in translation".

:::{admonition} Note
:class: note 
The library provides a `sanitize_string("client/server", string)`, which needs the user to specify whether they want a string intended for the client/server version, and the string to "sanitize", returning the appropriately-formatted version.
:::

Example of a correct call of the `dtaas_tui_client` program, which only runs the query:

```
dtaas_tui_client {\"ID\": \"727D2CAB7D0944E9830A9B413A6A33BF\", \"query\": \"SELECT \* FROM metadata WHERE capture_date = \'2013-11-14\'\"}
```

Example of a correct call of the `dtaas_tui_server` program, which runs the following Python script:

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

Note the extra backslashes:

```
dtaas_tui_server {\\\"ID\\\": \\\"727D2CAB7D0944E9830A9B413A6A33BF\\\", \\\"query\\\": \\\"SELECT \\* FROM metadata WHERE capture_date = \\\'2013-11-14\\\'\\\", \\\"script\\\": \\\"import imageio as iio\\\\nimport skimage as ski\\\\nimport matplotlib.pyplot as plt\\\\nimport os, sys\\\\nfrom multiprocessing import Pool\\\\n\\\\ndef rotate_image\(img_path\):\\\\n    os.makedirs\(\\\'rotated\\\', exist_ok=True\)\\\\n\\\\n    image = iio.imread\(uri=img_path\)\\\\n    rotated = ski.transform.rotate\(image=image, angle=45\)\\\\n\\\\n    fig, ax = plt.subplots\(1,2\)\\\\n    ax[0].imshow\(image\)\\\\n    ax[1].imshow\(rotated\)\\\\n\\\\n    path = f\\\'rotated/{os.path.basename\(img_path\)}\\\'\\\\n    fig.savefig\(path\)\\\\n\\\\n    return path\\\\n\\\\ndef main\(files_in\):\\\\n    with Pool\(\) as p:\\\\n        files_out = p.map\(rotate_image, files_in\)\\\\n\\\\n    return files_out\\\"}
```