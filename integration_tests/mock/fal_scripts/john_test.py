import pandas as pd

df: pd.DataFrame = ref(context.current_model.name)
print(df)
print(df.dtypes)
df.to_records()
write_to_source(df, "results", "john_source")
