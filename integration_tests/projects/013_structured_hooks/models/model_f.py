from fal.packages.environments.virtual_env import _BASE_VENV_DIR
from _fal_testing import create_model_artifact

# To determine whether this is a fal-created environment or not
# we'll check whether the executable that is running this script
# is located under _BASE_VENV_DIR

def model(dbt, fal):
    dbt.config(materialized='table')

    import sys
    from pathlib import Path

    executable_path = Path(sys.executable)
    environment_type = "venv" if _BASE_VENV_DIR in executable_path.parents else "local"

    df = dbt.ref("model_c")

    df["model_e_data"] = True

    create_model_artifact(fal, additional_data=f"Environment: {environment_type}")

    return df

