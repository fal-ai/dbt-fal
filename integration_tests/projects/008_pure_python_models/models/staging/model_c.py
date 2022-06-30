"""
DEPENDENCY: ref("model_b")
- ref('model_a')
"""
import os
from functools import reduce
from datetime import datetime

# does not get picked up, so we put it in the docstring
df = ref(*["model_b"])

df["my_bool"] = True

# BigQuery only:
# df["my_ts"] = datetime(2022, 6, 27, 17, 12, 25)

write_to_model(df)

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"

output = output + "\nModel dataframe name: {model_name}"
temp_dir = os.environ["temp_dir"]

write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".txt"]), "w")
write_dir.write(output)
write_dir.close()
