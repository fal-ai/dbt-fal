import pandas as pd
from functools import reduce
import os

model_name = context.current_model.name

output = f"Model name: {model_name}\nStatus: {context.current_model.status}"

path = reduce(os.path.join, [os.environ["temp_dir"], model_name + ".post_hook.txt"])
with open(path, "w") as file:
    file.write(output)
