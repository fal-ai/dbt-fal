import pandas as pd

model_name = context.current_model.name
df: pd.DataFrame = ref(model_name)

output = str(df)

print(output)
