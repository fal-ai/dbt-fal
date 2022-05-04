import os
from functools import reduce

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"
temp_dir = os.environ["temp_dir"]

write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".before.txt"]), "w")
write_dir.write(output)
write_dir.close()
