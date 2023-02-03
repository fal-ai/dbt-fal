from __future__ import annotations

from _fal_testing import create_dynamic_artifact

from fal.typing import *

create_dynamic_artifact(
    context,
    "Calculation result: "
    + str(context.arguments["left"] + context.arguments["right"]),
)
