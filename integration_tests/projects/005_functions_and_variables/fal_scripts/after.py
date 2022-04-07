from typing import Literal
import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name


def write_to_file(mode: Literal["w", "a"]):

    path = reduce(os.path.join, [os.environ["temp_dir"], model_name + ".after.txt"])
    with open(path, mode) as file:
        source_size = len(source("results", "some_source"))
        file.write(f"results.some_source size: {source_size}\n")


output = ""

df: pd.DataFrame = ref(model_name)
write_to_source(df, "results", "some_source", mode="overwrite")
source_size = len(source("results", "some_source"))
output += f"results.some_source size: {source_size}\n"

write_to_source(df, "results", "some_source", mode="append")
source_size = len(source("results", "some_source"))
output += f"results.some_source size: {source_size}\n"


path = reduce(os.path.join, [os.environ["temp_dir"], model_name + ".after.txt"])
with open(path, "w") as file:
    file.write(output)
