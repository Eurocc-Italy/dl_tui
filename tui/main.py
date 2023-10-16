import shutil
from pprint import pprint

# Available keys:
#  - _id        - caption
#  - id         - segmentation
#  - path       - area
#  - height     - iscrowd
#  - width      - bbox
#  - captured   - category_id
#  - category


print("  - searching for all entries with category 'hot dog'")
for entry in db.find():
    try:
        if entry["category"] == "hot dog":
            print(f"ID {entry['id']:06} : {entry['category']}")
    except KeyError:  # not all entries have a category
        print(f"WARNING: category not found for entry {entry['id']}")

print("\n  - saving .jpg relative to the following image:")
entry = db.find_one()
pprint(entry)
shutil.copy(entry["path"], f"{entry['id']:06}.jpg")
