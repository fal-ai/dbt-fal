import pandas
from fal.typing import *
from _fal_testing import create_model_artifact

df = pandas.DataFrame()

df["negative_data"] = [
    -1,
    -2,
    -3,
]
df["positive_data"] = [
    1,
    2,
    3,
]
write_to_model(df)
create_model_artifact(context)
