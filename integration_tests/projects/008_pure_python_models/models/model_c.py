"""
DEPENDENCY: ref("model_b")
- ref('mode_b')
"""
import pandas as pd

df: pd.DataFrame = ref("model_a")

# weird way to call, but added in the docstring
ref(*["model_b"])

df["my_bool"] = True

write_to_model(df)
