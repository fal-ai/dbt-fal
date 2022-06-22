import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name


output = ""

df: pd.DataFrame = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
output += f"my_float {df.my_float[0]}\n"

write_to_source(df, "results", "some_source", mode="overwrite")
source_size = len(source("results", "some_source"))
output += f"source size {source_size}\n"

write_to_source(df, "results", "some_source", mode="append")
source_size = len(source("results", "some_source"))
output += f"source size {source_size}\n"

for source in list_sources():
    output += (
        f"source {source.name}.{source.table_name} has {len(source.tests)} tests,"
        f" source status is {source.status}\n"
    )

for model in list_models():
    output += (
        f"model {model.name} has {len(model.tests)} tests,"
        f" model status is {model.status}\n"
    )

path = reduce(
    os.path.join, [os.environ["temp_dir"], model_name + ".write_to_source_twice.txt"]
)

with open(path, "w") as file:
    file.write(output)
