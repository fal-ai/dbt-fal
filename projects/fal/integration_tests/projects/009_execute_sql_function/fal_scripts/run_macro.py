import pandas as pd
import io
import os
from functools import reduce

query = """
    select {{ multiply_by_ten('my_int') }} as my_int_times_ten
    from {{ ref('execute_sql_model_two') }}
    """

df: pd.DataFrame = execute_sql(query)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
# Cast since some get float
df = df.astype({"my_int_times_ten": int})

output = f"\nModel dataframe first row:\n{df.iloc[0]}"
temp_dir = os.getenv("temp_dir", ".")
write_dir = open(
    reduce(os.path.join, [temp_dir, context.current_model.name + ".run_macro.txt"]),
    "w",
)
write_dir.write(output)
write_dir.close()
