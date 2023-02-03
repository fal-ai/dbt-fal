from __future__ import annotations

import os
from functools import reduce

import pandas as pd

model_name = context.current_model.name
df: pd.DataFrame = ref(model_name)

output = str(df)

path = reduce(os.path.join, [os.environ["temp_dir"], model_name + ".after.txt"])
with open(path, "w") as file:
    file.write(output)
