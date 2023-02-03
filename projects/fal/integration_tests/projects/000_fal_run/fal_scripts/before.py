from __future__ import annotations

import io
import os
from functools import reduce

import pandas as pd

model_name = context.current_model.name

output = f"Model name: {model_name}"
output = output + f"\nStatus: {context.current_model.status}"

df: pd.DataFrame = ref(model_name)
buf = io.StringIO()
df.info(buf=buf, memory_usage=False)
info = buf.getvalue()

output = output + f"\nModel dataframe information:\n{info}"
temp_dir = os.environ["temp_dir"]
print(temp_dir)
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".before.txt"]), "w")
write_dir.write(output)
write_dir.close()
