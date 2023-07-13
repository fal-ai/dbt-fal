from fal.dbt.typing import *
from _fal_testing import create_dynamic_artifact

import pyjokes

create_dynamic_artifact(context, f"PyJokes version: {pyjokes.__version__}")

