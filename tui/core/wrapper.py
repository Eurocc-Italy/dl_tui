import sys
from tui.core import hpc
from tui.core.sql2mongo import sql_to_mongo
import json

sql_query = json.loads(sys.argv[1])["query"]
mongo_query = sql_to_mongo(sql_query)

files_in = []
for entry in hpc.CLIENT["datalake"]["metadata"].find(mongo_query):
    files_in.append(entry["path"])
files_out = []

sys.path.append(hpc.VM_SUBMIT_DIR)
import script

script.main(files_in, files_out)

for file in files_out:
    print(file)
