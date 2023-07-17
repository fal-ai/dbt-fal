import pandas as pd
import io
import os
from functools import reduce

df: pd.DataFrame = execute_sql('SELECT * FROM {{ ref("execute_sql_model_one")}}')

buf = io.StringIO()
df.info(buf=buf, memory_usage=False)
info = buf.getvalue()
output = f"\nModel dataframe information:\n{info}"
temp_dir = os.getenv("temp_dir", ".")
write_dir = open(
    reduce(
        os.path.join, [temp_dir, context.current_model.name + ".query_other_model.txt"]
    ),
    "w",
)
write_dir.write(output)
write_dir.close()
