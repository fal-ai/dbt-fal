import pandas as pd
from utils import model_info

info_str = model_info.model_info_str(context.current_model)

df: pd.DataFrame = ref(context.current_model.name)
print(info_str)
print(df)
print(df.dtypes)
write_to_source(df, "results", "john_source")
