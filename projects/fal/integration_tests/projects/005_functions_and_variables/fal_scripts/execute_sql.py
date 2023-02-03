from __future__ import annotations

from _fal_testing.utils import create_dynamic_artifact

from fal.typing import *

df = execute_sql("SELECT 1 as a, 2 as b, 3 as c")

assert 2 == df["b"][0]

df.info()

create_dynamic_artifact(context)
