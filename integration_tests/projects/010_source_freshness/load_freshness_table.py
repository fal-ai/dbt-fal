from datetime import datetime as dt, timezone as tz
from pandas import DataFrame
from fal import FalDbt
from sys import argv

project_dir = argv[1] if len(argv) >= 2 else "."
profiles_dir = argv[2] if len(argv) >= 3 else ".."

faldbt = FalDbt(project_dir=project_dir, profiles_dir=profiles_dir)

# 10 rows
df = DataFrame({"loaded_at": dt.now(tz=tz.utc).isoformat(), "info": range(0, 10)})

print(df)
faldbt.write_to_source(df, "freshness_test", "freshness_table", mode="overwrite")

from time import sleep

# Let BigQuery cache load it
sleep(10)

print("Lodaded")
