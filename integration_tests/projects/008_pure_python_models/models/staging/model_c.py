"""
DEPENDENCY: ref("model_b")
- ref('model_a')
"""
import os
from functools import reduce

# weird way to call, but added in the docstring
df = ref(*["model_b"])

df["my_bool"] = True

write_to_model(df)

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"

output = output + "\nModel dataframe name: {model_name}"
temp_dir = os.environ["temp_dir"]

write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".txt"]), "w")
write_dir.write(output)
write_dir.close()
