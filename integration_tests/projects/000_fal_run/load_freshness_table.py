from datetime import datetime as dt, timezone as tz
from pandas import DataFrame
from fal import FalDbt
import sys

print(sys.argv)

faldbt = FalDbt(project_dir=sys.argv[1] or ".", profiles_dir=sys.argv[2] or "..")

# 10 rows
df = DataFrame({"loaded_at": dt.now(tz=tz.utc).isoformat(), "info": range(0, 10)})

print(df)
faldbt.write_to_source(df, "freshness_test", "freshness_table", mode="overwrite")

print("Lodaded")
