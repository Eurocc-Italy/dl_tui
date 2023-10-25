import sys
from tui.core import hpc
from tui.core.sql2mongo import sql_to_mongo
import json
import argparse

test_query = {
    "--query": """SELECT * FROM datalake WHERE category = motorcycle""",
    "--script": """$(cat script.py)""",
}

# Parsing API input
parser = argparse.ArgumentParser()
parser.add_argument("--query", type=str, required=True)
parser.add_argument("--script", type=str, required=False)
args = parser.parse_args()

# Reading SQL query from API input and converting to MongoDB query
sql_query = args.query
mongo_query = sql_to_mongo(sql_query)

# Reading script from API input and converting to local file
with open("script.py", "w") as f:
    for line in args.script:
        f.write(line)

# Generating "input" file list according to user query and initializing "output" files list
files_in = []
for entry in hpc.CLIENT["datalake"]["metadata"].find(mongo_query):
    files_in.append(entry["path"])
files_out = []

# Running user-provided Python script
sys.path.append(hpc.VM_SUBMIT_DIR)
import script

script.main(files_in, files_out)

# Saving output files and telling user where to find them
for file in files_out:
    print(file)
