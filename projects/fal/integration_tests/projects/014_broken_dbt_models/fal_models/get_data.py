from __future__ import annotations

import pandas
from _fal_testing import create_model_artifact

from fal.typing import *

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
