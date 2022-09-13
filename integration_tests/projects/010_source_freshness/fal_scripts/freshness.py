from typing import List
import os
from fal import DbtSource

output = ""
# TODO add real test for freshness
sources: List[DbtSource] = list_sources()
for node in sources:
    if node.freshness:
        # NOTE: removing the namespace prefix
        output += f"({node.name}, {node.table_name.split('__ns__')[1]}) {node.freshness.status}\n"

temp_dir = os.environ["temp_dir"]
write_dir = open(os.path.join(temp_dir, "GLOBAL.freshness.txt"), "w")
write_dir.write(output)
write_dir.close()
