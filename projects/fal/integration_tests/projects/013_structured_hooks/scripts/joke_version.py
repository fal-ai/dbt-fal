from __future__ import annotations

import pyjokes
from _fal_testing import create_dynamic_artifact

from fal.typing import *

create_dynamic_artifact(context, f"PyJokes version: {pyjokes.__version__}")
