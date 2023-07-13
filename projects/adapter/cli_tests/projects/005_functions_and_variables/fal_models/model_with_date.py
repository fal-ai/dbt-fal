from fal.dbt.typing import *
from _fal_testing.utils import create_model_artifact

import pandas as pd
import datetime as dt


arr = [dt.datetime(2022, 1, 1, 14, 50, 59), dt.datetime.now()]
df = pd.DataFrame(
    {
        # NOTE: timestamp without time zone fail to write to Snowflake
        # https://github.com/snowflakedb/snowflake-connector-python/issues/600#issuecomment-844524183
        "my_datetime": map(lambda d: pd.Timestamp(d, unit="ms", tz="UTC"), arr),
        "my_date": map(lambda d: d.date(), arr),
        "my_time": map(lambda d: d.time(), arr),
    }
)

df.info()

model_name = context.current_model.name

write_to_model(df)
df = ref(model_name)
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df.info()


# HACK: Snowflake returns without the time zone information
df["my_datetime"] = df["my_datetime"].apply(
    lambda d: d.tz_localize("UTC") if not d.tz else d
)
output = f"my_datetime: {df['my_datetime'][0].isoformat()}"
output += f"\nmy_date: {df['my_date'][0].isoformat()}"
output += f"\nmy_time: {df['my_time'][0].isoformat()}"
create_model_artifact(context, additional_data=output)
