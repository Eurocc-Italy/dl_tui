from tui.core.db import filter

data = filter(database="COCO", collection="COCO", filter_dict={"category": "hot dog"})

for i in data:
    print(data)
