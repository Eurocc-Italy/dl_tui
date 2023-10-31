"""
Wrapper which does the following:

  1. Takes user SQL query and converts it to Mongo spec using custom sqlparse codebase (@lbabetto/sqlparse)
  2. Runs query and retrieves the matching files, generating a list with the paths to the files
  3. Takes the user-provided python script and runs it locally, feeding the input files list to the `main` function
  4. Retrieves the output files from the `main` function and saves the files (of any kind, user-defined) in a zipped 
  archive which the user can then download

NOTE: User script must have a `main(input_files)` function which takes as argument a lists of paths with the
input files and returns a list of paths with the output/processed files.

Author: @lbabetto
"""

import os
import sys
import shutil
from pymongo import MongoClient
from sqlparse.builders.mongo_builder import MongoQueryBuilder
from argparse import ArgumentParser

from dtaas import utils

config = utils.load_config()

import logging

logging.basicConfig(
    filename=config["LOGGING"]["logfile"],
    format=config["LOGGING"]["format"],
    level=config["LOGGING"]["level"].upper(),
)

MONGODB_URI = f"mongodb://{config['MONGO']['user']}:{config['MONGO']['password']}@{config['MONGO']['ip']}:{config['MONGO']['port']}/"
CLIENT = MongoClient(MONGODB_URI)
logging.info(f"Connected to client: {MONGODB_URI}")

# parsing input, expecting something like `python wrapper.py --query """[...]""" --script """[...]"""`
parser = ArgumentParser()
parser.add_argument("--query", type=str, required=True)
parser.add_argument("--script", type=str, required=False)
args = parser.parse_args()
logging.debug(f"API input (wrapper): {args}")

# SQL query
logging.info(f"User query: {args.query}")

# user script (will be deleted at the end of the program)
if args.script:
    logging.info(f"User script: \n{args.script}")
    with open("script.py", "w") as f:
        f.write(args.script)

# converting SQL query to MongoDB spec
builder = MongoQueryBuilder()
try:
    mongo_query = builder.parse_and_build(args.query)
    query_filters = mongo_query[0]
    try:
        query_fields = mongo_query[1]["fields"]
    except KeyError:
        query_fields = {}
except IndexError:
    logging.debug("Missing WHERE clause from SQL query, returning all data in the database.")
    query_fields = "{}"
    query_filters = "{}"
logging.info(f"MongoDB query filter: {query_filters}")
logging.info(f"MongoDB query fields: {query_fields}")

# Generating "input" file list according to user query and initializing "output" files list
files_in = []
for entry in CLIENT[config["MONGO"]["database"]][config["MONGO"]["collection"]].find(query_filters, query_fields):
    files_in.append(entry["path"])
logging.debug(f"Query results: {files_in}")
files_out = []

try:
    # Running user-provided Python script
    sys.path.insert(0, ".")
    from script import main

    files_out = main(files_in)
except ModuleNotFoundError:
    logging.info("No processing script found, returning queried file list as output.")
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
    shutil.make_archive("results", "zip", "RESULTS")
    shutil.rmtree("RESULTS")
    logging.info("Processed files available in the results.zip archive")

# removing temporary script
os.remove("script.py")
