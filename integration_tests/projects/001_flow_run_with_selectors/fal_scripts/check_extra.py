import pandas as pd
import io
import os
from functools import reduce

model_name = context.current_model.name
df: pd.DataFrame = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns


if hasattr(df, "extra_col"):
    output = f"extra_col: {df.extra_col[0]}\n"
else:
    output = "no extra_col\n"

temp_dir = os.environ["temp_dir"]
write_dir = open(reduce(os.path.join, [temp_dir, model_name + ".check_extra.txt"]), "w")
write_dir.write(output)
write_dir.close()
