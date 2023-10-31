"""
Wrapper which does the following:

  1. Takes user SQL query and converts it to Mongo spec using custom sqlparse codebase (@lbabetto/sqlparse)
  2. Runs query and retrieves the matching files, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it locally, feeding the input files list to the `main` function
  4. Retrieves the output files from the `main` function and saves the files (of any kind, user-defined) in a zipped 
  archive which the user can then download

NOTE: User script must have a `main(input_files)` function which takes as argument a lists of paths with the
input files and returns a list of paths with the output/processed files. This `main` function will be the one
which will be run on HPC.

Author: @lbabetto
"""

import os, sys
import shutil
from dtaas.launcher import CLIENT, SUBMIT_DIR
from sqlparse.builders.mongo_builder import MongoQueryBuilder

from dtaas import utils

config = utils.load_config()

import logging

logging.basicConfig(
    filename=config["LOGGING"]["logfile"],
    format=config["LOGGING"]["format"],
    level=config["LOGGING"]["level"].upper(),
)

# Reading SQL query from QUERY file, as parsed by launcher.py...
with open("QUERY", "r") as f:
    sql_query = f.readline()

# ...and converting to MongoDB query
builder = MongoQueryBuilder()
try:
    mongo_query = builder.parse_and_build(sql_query)
    query_filters = mongo_query[0]
    try:
        query_fields = mongo_query[1]["fields"]
    except KeyError:
        query_fields = {}
except IndexError:
    logging.warning("Missing WHERE clause from SQL query, returning ALL data in the database.")
    query_fields = "{}"
    query_filters = "{}"
logging.info(f"MongoDB query filter: {query_filters}")
logging.info(f"MongoDB query fields: {query_fields}")

# Generating "input" file list according to user query and initializing "output" files list
files_in = []
for entry in CLIENT[config["MONGO"]["database"]][config["MONGO"]["collection"]].find(query_filters, query_fields):
    files_in.append(entry["path"])
files_out = []
logging.debug(f"Query results: {files_in}")

try:
    # Running user-provided Python script
    sys.path.insert(0, SUBMIT_DIR)
    from script import main

    files_out = main(files_in)
except ModuleNotFoundError:
    logging.info("User-provided input not found, returning queried file list as output.")
    files_out = files_in
except ImportError:
    logging.error("Did not find `main` function in user-provided script. ABORTING.")
    exit()

if type(files_out) != list:
    raise TypeError("`main` function does not return a list of paths.")

# Saving output files and telling user where to find them
logging.debug(f"Processed results: {files_out}")

if files_out != []:
    os.makedirs(f"RESULTS", exist_ok=True)
    for file in files_out:
        shutil.copy(file, f"RESULTS/{os.path.basename(file)}")
    os.system("zip -r results.zip RESULTS/*")
    shutil.rmtree("RESULTS")
    logging.info("Processed files available in the results.zip archive")
