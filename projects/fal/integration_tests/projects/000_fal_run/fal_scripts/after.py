import pandas as pd
import io
import os
from functools import reduce

model_name = context.current_model.name
model_alias = context.current_model.alias

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"

df: pd.DataFrame = ref(model_name)
buf = io.StringIO()
df.info(buf=buf, memory_usage=False)
info = buf.getvalue()

output = output + f"\nModel dataframe information:\n{info}"
output = output + f"\nModel alias is {model_alias}"
alias_no_namespace = model_alias.split('__ns__')[1]
output = output + f"\nModel alias without namespace is {alias_no_namespace}"
temp_dir = os.getenv("temp_dir", ".")
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".after.txt"]), "w")
write_dir.write(output)
write_dir.close()
