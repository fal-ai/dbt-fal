import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name


output = ""

df: pd.DataFrame = ref(model_name)
output += f"my_float {df.my_float[0]}\n"

df.my_float = 2.3
write_to_model(df, mode="overwrite")

df: pd.DataFrame = ref(model_name)
output += f"my_float {df.my_float[0]}\n"
output += f"size {len(df)}\n"


path = reduce(
    os.path.join, [os.environ["temp_dir"], model_name + ".complete_other_model.txt"]
)
with open(path, "w") as file:
    file.write(output)