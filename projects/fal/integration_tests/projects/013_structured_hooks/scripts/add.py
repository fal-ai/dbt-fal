from fal.typing import *
from _fal_testing import create_dynamic_artifact

create_dynamic_artifact(
    context,
    "Calculation result: "
    + str(context.arguments["left"] + context.arguments["right"]),
)
