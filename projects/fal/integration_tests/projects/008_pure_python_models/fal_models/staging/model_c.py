"""
DEPENDENCY:
# not really used, but we put it to make sure it is picked up
- ref('model_a')
"""
from __future__ import annotations

from _fal_testing.utils import create_model_artifact

from fal.typing import *

df = ref("model_b")

df["my_bool"] = True

write_to_model(df)

create_model_artifact(context)
