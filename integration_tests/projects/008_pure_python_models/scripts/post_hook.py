import pandas as pd
from functools import reduce
import os
import time
from fal.context import *

# Delay just a little bit in order to make sure the file is created
# just after the model file.
time.sleep(0.05)


model_name = context.current_model.name

output = f"Model name: {model_name}\nStatus: {context.current_model.status}"

path = reduce(os.path.join, [os.environ["temp_dir"], model_name + ".post_hook.txt"])
with open(path, "w") as file:
    file.write(output)
