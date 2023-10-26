"""
Wrapper which does the following:

  1. Takes user SQL query and converts it to Mongo spec
  2. Runs query on HPC and retrieves the matching files, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it on HPC, feeding the input files list to the "main" function
  4. Saves the processed files (of any kind, user-defined) in a directory which the user can then download

NOTE: User script must have a "main(input_files, output_files)" function which takes as arguments 2 lists, 
one with the paths to input files and one with the path of the output/processed files. That will be the function
which will be ran on HPC.

!!! WIP !!!

Author: @lbabetto
"""

import sys
from tui.core import hpc
from tui.core.sql2mongo import sql_to_mongo
import argparse

test_query = {
    "--query": """SELECT * FROM datalake WHERE category = motorcycle AND width > 640""",
    "--script": """$(cat script.py)""",
}

# Parsing API input, requires --query keyword, with optional --script
parser = argparse.ArgumentParser()
parser.add_argument("--query", type=str, required=True)
parser.add_argument("--script", type=str, required=False)
args = parser.parse_args()

# Reading SQL query from API --query input and converting to MongoDB query
sql_query = args.query
mongo_query = sql_to_mongo(sql_query)

# Reading script from API input and converting to local file (will be put in HPC workdir when called by hpc module)
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
from script import main

result = main(files_in, files_out)
if result == "xnWRq#XjhbpcQwDcgRGMQBPP!h2hxMtuW63UyB4buEH&@B52E&C^&L3HcDY7QSje":
    print("NO SCRIPT FOUND")

# Saving output files and telling user where to find them
for file in files_out:
    print(file)
