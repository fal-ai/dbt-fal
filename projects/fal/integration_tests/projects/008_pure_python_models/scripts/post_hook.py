from __future__ import annotations

import time

from _fal_testing import create_dynamic_artifact

from fal.typing import *

# Delay just a little bit in order to make sure the file is created
# just after the model file.
time.sleep(0.05)

create_dynamic_artifact(context)
