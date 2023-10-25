import os
import sys


def read_cl():
    """Read command line input from user

    Returns
    -------
    dict
        dictionary containing the flags/keywords sent by the user
    """
    # available flags
    flags = {
        "info": False,
        "database": None,
        "collection": None,
        "filters": {},
        "script": None,
    }

    curr_flag = ""
    for key in sys.argv[1:]:
        if key.startswith("--"):
            curr_flag = key.lstrip("-")
        else:
            if curr_flag == "filters":
                sep = key.find("=")
                flags[curr_flag].update({key[:sep]: key[sep + 1 :]})
            elif curr_flag == "info":
                flags[curr_flag] = True
            else:
                flags[curr_flag] = key

    return flags


FILE_LIST = []


def generate_file_list(client, flags):
    """Generates a file list matching the user query

    Parameters
    ----------
    client : MongoClient
        MongoDB client containing the database
    flags : dict
        dictionary (generated from read_cl function) containing the db info and query
    """

    for entry in client[flags["database"]][flags["collection"]].find(flags["filters"]):
        FILE_LIST.append(entry["path"])


def print_info(client, flags):
    """Prints info relative to the requested database

    Parameters
    ----------
    client : MongoClient
        MongoDB client containing the database
    flags : dict
        dictionary (generated from read_cl function) containing the db info and query
    """
    print("\n" + "─" * os.get_terminal_size()[0])
    print(f"\nDATABASE: {flags['database']}")
    print(f"Collections:")
    for coll in client[flags["database"]].list_collection_names():
        print(f"  - {coll}")

    print("\n" + "─" * os.get_terminal_size()[0])
    for db in flags["database"]:
        for coll in client[db].list_collection_names():
            keys = {}
            for entry in client[db][coll].find():
                for key in entry.keys():
                    if key not in keys.keys():
                        keys[key] = 0
                    else:
                        keys[key] += 1

            print(f"\nAvailable keys (DATABASE: {db} | COLLECTION: {coll}):\n")
            print("  key                entries")
            print("  --------------------------")
            for key, count in keys.items():
                print(f"  {key:20}{count}")
