import os
from functools import reduce

model_name = context.current_model.name if context.current_model else "GLOBAL"

output = f"Model name: {model_name}"

temp_dir = os.getenv("temp_dir", ".")
print(temp_dir)
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".before_b.txt"]), "w")
write_dir.write(output)
write_dir.close()
