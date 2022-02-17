import pandas as pd

df: pd.DataFrame = ref("john_table")
print(df)
print(df.dtypes)
df.to_records()
write_to_source(df, "results", "john_source")
