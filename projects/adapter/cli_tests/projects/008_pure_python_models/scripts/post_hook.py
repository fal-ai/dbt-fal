import time
from fal.dbt.typing import *
from _fal_testing import create_dynamic_artifact

# Delay just a little bit in order to make sure the file is created
# just after the model file.
time.sleep(0.05)

create_dynamic_artifact(context)
