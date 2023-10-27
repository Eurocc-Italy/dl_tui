"""
Wrapper which does the following:

  1. Takes user SQL query and converts it to Mongo spec using custom sqlparse codebase (@lbabetto/sqlparse)
  2. Runs query on HPC and retrieves the matching files, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it on HPC, feeding the input files list to the "main" function
  4. Saves the processed files (of any kind, user-defined) in a directory which the user can then download

NOTE: User script must have a "main(input_files)" function which takes as argument a lists of paths with the
input files and returns a list of paths with the output/processed files. This "main" function will be the one
which will be run on HPC.

Author: @lbabetto
"""

import sys
from dtaas import launcher
from sqlparse.builders.mongo_builder import MongoQueryBuilder

import logging

logger = logging.getLogger(__name__)


# Reading SQL query from QUERY file, as parsed by launcher.py...
with open("QUERY", "r") as f:
    sql_query = f.readline()

# ...and converting to MongoDB query
builder = MongoQueryBuilder()
mongo_query = builder.parse_and_build(sql_query)
query_filters = mongo_query[0]
query_fields = mongo_query[1]
logger.info(f"MongoDB query filter: {query_filters}")
logger.info(f"MongoDB query fields: {query_fields}")

# Generating "input" file list according to user query and initializing "output" files list
files_in = []
for entry in launcher.CLIENT["datalake"]["metadata"].find(query_filters):
    files_in.append(entry["path"])
files_out = []
logger.debug(f"Query results:")
for file in files_in:
    logger.debug(file)

# Running user-provided Python script
sys.path.insert(0, launcher.VM_SUBMIT_DIR)

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
