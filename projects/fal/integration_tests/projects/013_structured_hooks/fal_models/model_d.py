from __future__ import annotations

import pyjokes
from _fal_testing import create_model_artifact

from fal.typing import *

df = ref("model_c")

df["model_e_data"] = True

write_to_model(df)

create_model_artifact(context, f"PyJokes version: {pyjokes.__version__}")
