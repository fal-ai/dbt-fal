import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name
df: pd.DataFrame = ref(model_name)

output = str(df)

path = reduce(os.path.join, [os.getenv("temp_dir", "."), model_name + ".after.txt"])
with open(path, "w") as file:
    file.write(output)
