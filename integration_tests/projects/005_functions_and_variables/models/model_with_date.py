import pandas as pd
from datetime import datetime as dt
import os

df = pd.DataFrame({"my_date": [dt(2022, 1, 1, 0, 0, 0), dt.now()]})

df.info()

write_to_model(df)

model_name = context.current_model.name
df = ref(model_name)
# TODO: Snowflake gets an int representation
df.columns = df.columns.str.lower()  # Snowflake has uppercase columns
df.info()
output = f"my_date: {df['my_date'][0]}"
temp_dir = os.environ["temp_dir"]
with open(os.path.join(temp_dir, model_name + ".txt"), "w") as file:
    file.write(output)
