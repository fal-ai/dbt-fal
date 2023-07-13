from fal.dbt.typing import *
from _fal_testing import create_dynamic_artifact

create_dynamic_artifact(
    context,
    "Arguments: "
    + ", ".join(f"{key}={value!r}" for key, value in context.arguments.items()),
)
