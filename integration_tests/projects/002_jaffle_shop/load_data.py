import pandas as pd
import io
import os
from functools import reduce

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"

df: pd.DataFrame = ref(model_name)
buf = io.StringIO()
df.info(buf=buf, memory_usage=False)
info = buf.getvalue()

output = output + f"\nModel dataframe information:\n{info}"
temp_dir = os.environ["temp_dir"]
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".load_data.py"]), "w")
write_dir.write(output)
write_dir.close()
