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
from sqlparse.builders.mongo_builder import MongoQueryBuilder
import argparse

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler("wrapper.log")

# Create formatters and add it to handlers
c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.DEBUG)
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

test_query = {
    "--query": """SELECT * FROM datalake WHERE category = motorcycle AND width > 600""",
    "--script": """$(cat script.py)""",
}

# Parsing API input, requires --query keyword, with optional --script
parser = argparse.ArgumentParser()
parser.add_argument("--query", type=str, required=True)
parser.add_argument("--script", type=str, required=False)
args = parser.parse_args()
logger.debug(f"API input: {args}")

# Reading SQL query from API --query input and converting to MongoDB query
sql_query = args.query
builder = MongoQueryBuilder()
mongo_query = builder.parse_and_build(sql_query)
query_filters = mongo_query[0]
query_fields = mongo_query[1]
logger.info(f"MongoDB query filter: {query_filters}")
logger.info(f"MongoDB query fields: {query_fields}")


# Reading script from API input and converting to local file (will be put in HPC workdir when called by hpc module)
with open("script.py", "w") as f:
    for line in args.script:
        f.write(line)

# Generating "input" file list according to user query and initializing "output" files list
files_in = []
for entry in hpc.CLIENT["datalake"]["metadata"].find(query_filters):
    files_in.append(entry["path"])
files_out = []
logger.debug(f"Query results:")
for file in files_in:
    logger.debug(file)

# Running user-provided Python script
sys.path.insert(0, hpc.VM_SUBMIT_DIR)

try:
    from script import main

    files_out = main(files_in)
except:
    msg = "main function not found in script, or does not accept a list of paths as input."
    print(msg)
    logger.error(msg)

if type(files_out) != list:
    raise TypeError("main function does not return a list of paths.")

if files_out == "xnWRq#XjhbpcQwDcgRGMQBPP!h2hxMtuW63UyB4buEH&@B52E&C^&L3HcDY7QSje":
    logger.info("No script found, returning queried file list as output.")
    files_out = files_in

# Saving output files and telling user where to find them
logger.debug(f"Processed results:")
for file in files_out:
    logger.debug(file)
for file in files_out:
    print(file)
