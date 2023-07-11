import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name


output = ""

df: pd.DataFrame = ref(model_name).fillna(0)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df = df.astype({"my_int": float})
output += f"my_int {df.my_int[0]}\n"

df.my_int = 3
write_to_model(df, mode="append")

float_df = df.astype({"my_int": float})
write_to_model(float_df)

write_to_model(df)  # default: overwrite

df: pd.DataFrame = ref(model_name).fillna(0)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df = df.astype({"my_int": float})
output += f"my_int {df.my_int[0]}\n"
output += f"size {len(df)}\n"


path = reduce(
    os.path.join, [os.getenv("temp_dir", "."), model_name + ".complete_model.txt"]
)
with open(path, "w") as file:
    file.write(output)
