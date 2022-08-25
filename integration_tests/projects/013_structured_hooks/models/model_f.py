import sys
from pathlib import Path

from fal.typing import *
from fal.packages.environment import _BASE_VENV_DIR
from _fal_testing import create_model_artifact

# To determine whether this is a fal-created environment or not
# we'll check whether the executable that is running this script
# is located under _BASE_VENV_DIR

executable_path = Path(sys.executable)
environment_type = "venv" if _BASE_VENV_DIR in executable_path.parents else "local"

df = ref("model_c")

df["model_e_data"] = True

write_to_model(df)

create_model_artifact(context, additional_data=f"Environment: {environment_type}")
