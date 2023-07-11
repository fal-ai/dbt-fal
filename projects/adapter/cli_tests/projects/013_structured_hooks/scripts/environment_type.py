from fal.typing import *
from _fal_testing import create_dynamic_artifact, get_environment_type

create_dynamic_artifact(context, additional_data=f"Environment: {get_environment_type()}")
