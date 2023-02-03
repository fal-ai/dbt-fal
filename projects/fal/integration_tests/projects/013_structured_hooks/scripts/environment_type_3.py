from __future__ import annotations

from _fal_testing import create_dynamic_artifact, get_environment_type

from fal.typing import *

create_dynamic_artifact(
    context, additional_data=f"Environment: {get_environment_type()}"
)
