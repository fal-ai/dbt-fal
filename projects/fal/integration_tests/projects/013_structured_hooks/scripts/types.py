from __future__ import annotations

from _fal_testing import create_dynamic_artifact

from fal.typing import *

create_dynamic_artifact(
    context,
    "Arguments: "
    + ", ".join(f"{key}={value!r}" for key, value in context.arguments.items()),
)
