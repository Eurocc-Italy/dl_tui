import shutil
from pprint import pprint

import logging

logger = logging.getLogger(__name__)

from core.db import run_query

# Available keys:
#  - _id        - caption
#  - id         - segmentation
#  - path       - area
#  - height     - iscrowd
#  - width      - bbox
#  - captured   - category_id
#  - category


def query(db):
    print("  - searching for all entries with category 'hot dog'")
    for entry in db.find():
        try:
            if entry["category"] == "hot dog":
                print(f"ID {entry['id']:06} : {entry['category']}")
        except KeyError:  # not all entries have a category
            pass

    print("\n  - saving .jpg relative to the following image:")
    entry = db.find_one()
    pprint(entry)
    shutil.copy(entry["path"], f"{entry['id']:06}.jpg")


run_query("COCO", query)
