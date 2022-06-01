import os
from functools import reduce
from main_check_2 import main_check

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}\n"

output += f"top name: {__name__}\n"
output = main_check(output)
if __name__ == "__main__":
    output += "passed main if\n"

temp_dir = os.environ["temp_dir"]
print(temp_dir)
write_dir = open(
    reduce(os.path.join, [temp_dir, model_name + ".middle_script.txt"]), "w"
)
write_dir.write(output)
write_dir.close()
