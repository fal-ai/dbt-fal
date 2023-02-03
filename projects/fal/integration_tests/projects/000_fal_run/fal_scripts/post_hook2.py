from __future__ import annotations

import io
import os
from functools import reduce

import pandas as pd

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
temp_dir = os.environ["temp_dir"]
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".post_hook2.txt"]), "w")
write_dir.write(output)
write_dir.close()
