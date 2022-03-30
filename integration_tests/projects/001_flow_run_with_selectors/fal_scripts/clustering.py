import pandas as pd
import io
import os
from functools import reduce

model_name = context.current_model.name


for i in range(10):
    print("running clustering script.. for model name: " + model_name)

temp_dir = os.environ["temp_dir"]
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".clustering.py"]), "w")
write_dir.write(model_name)
write_dir.close()
