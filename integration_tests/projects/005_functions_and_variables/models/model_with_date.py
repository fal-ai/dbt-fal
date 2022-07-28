from fal.typing import *
from _fal_testing.utils import create_model_artifact

import pandas as pd
import datetime as dt


arr = [dt.datetime(2022, 1, 1, 14, 50, 59), dt.datetime.now()]
df = pd.DataFrame(
    {
        "my_datetime": arr,
        "my_date": map(lambda d: d.date(), arr),
        "my_time": map(lambda d: d.time(), arr),
    }
)

df.info()

# TODO: Snowflake sends an int representation for datetime
write_to_model(df)

model_name = context.current_model.name
df = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df.info()
output = f"my_datetime: {df['my_datetime'][0]}"
output += f"\nmy_date: {df['my_date'][0]}"
output += f"\nmy_time: {df['my_time'][0]}"
create_model_artifact(context, additional_data=output)
