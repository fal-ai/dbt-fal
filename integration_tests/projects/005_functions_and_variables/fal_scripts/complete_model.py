import pandas as pd
from functools import reduce
from sqlalchemy import Integer
import os

model_name = context.current_model.name


output = ""

df: pd.DataFrame = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
output += f"my_float {df.my_float[0]}\n"

df.my_float = 2.3
write_to_model(df, mode="append")
write_to_model(df, dtype={"my_float": Integer})
write_to_model(df)  # default: overwrite

df: pd.DataFrame = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
output += f"my_float {df.my_float[0]}\n"
output += f"size {len(df)}\n"


path = reduce(
    os.path.join, [os.environ["temp_dir"], model_name + ".complete_model.txt"]
)
with open(path, "w") as file:
    file.write(output)
