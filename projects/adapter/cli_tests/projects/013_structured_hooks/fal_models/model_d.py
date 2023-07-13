import pyjokes
from fal.dbt.typing import *
from _fal_testing import create_model_artifact

df = ref("model_c")

df["model_e_data"] = True

write_to_model(df)

create_model_artifact(context, f"PyJokes version: {pyjokes.__version__}")
