import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name


output = ""

df: pd.DataFrame = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns

output += f"my_float {df.my_float[0]}\n"

table_prefix = f"ns__{ os.environ.get('DB_NAMESPACE', '') }__ns__"

write_to_source(df, "results", table_prefix + "some_source", mode="overwrite")
source_size = len(source("results", table_prefix + "some_source"))
output += f"source size {source_size}\n"

write_to_source(df, "results", table_prefix + "some_source", mode="append")
source_size = len(source("results", table_prefix + "some_source"))
output += f"source size {source_size}\n"

for source in list_sources():
    output += (
        # NOTE: removing the namespace prefix
        f"source {source.name}.{source.table_name.split('__ns__')[1]} has {len(source.tests)} tests,"
        f" source status is {source.status}\n"
    )

for model in list_models():
    output += (
        f"model {model.name} has {len(model.tests)} tests,"
        f" model status is {model.status}\n"
    )

path = reduce(
    os.path.join, [os.getenv("temp_dir", "."), model_name + ".write_to_source_twice.txt"]
)

with open(path, "w") as file:
    file.write(output)
