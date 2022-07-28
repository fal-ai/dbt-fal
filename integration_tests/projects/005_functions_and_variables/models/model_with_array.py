from fal.typing import *
from _fal_testing.utils import create_model_artifact

import pandas as pd

df = pd.DataFrame(
    {
        "my_array": [["some", "other"], []],
        "other_array": [[1, 2, 3], []],
    }
)
df.info()

model_name = context.current_model.name

write_to_model(df)
df = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df.info()

write_to_model(df)
df = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df.info()

output = f"my_array: {list(df['my_array'][0])}"
output += f"\nother_array: {list(df['other_array'][0])}"
create_model_artifact(context, additional_data=output)
