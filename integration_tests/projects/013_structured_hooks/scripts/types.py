from fal.typing import *
from _fal_testing import create_dynamic_artifact

create_dynamic_artifact(
    context,
    "Types: "
    + ", ".join(f"{key}={type(value).__name__}" for key, value in context.arguments.items()),
)
