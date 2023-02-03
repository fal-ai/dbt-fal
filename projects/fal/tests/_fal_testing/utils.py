import os
import sys
import inspect
from pathlib import Path


def create_artifact(context, suffix, additional_data=None):
    model = context.current_model
    model_name = model.name if model else "GLOBAL"
    model_status = model.status if model else None

    output = f"Model name: {model_name}"
    output += f"\nStatus: {model_status}"
    output += f"\nModel dataframe name: {model_name}"
    if additional_data:
        output += f"\n{additional_data}"

    create_file(output, Path(model_name).with_suffix(suffix))


def create_file(output, file_name):
    temp_dir = Path(os.environ["temp_dir"])
    temp_file = temp_dir / file_name
    temp_file.write_text(output)


def create_model_artifact(context, additional_data=None):
    create_artifact(context, ".txt", additional_data)


def create_script_artifact(context, prompt, additional_data=None):
    create_artifact(context, f".{prompt}.txt", additional_data)


def create_dynamic_artifact(context, additional_data=None):
    _, outer_frame, *_ = inspect.stack()
    return create_script_artifact(
        context, Path(outer_frame.filename).stem, additional_data
    )


def get_environment_type():
    # To determine whether this is a fal-created environment or not
    # we'll check whether the executable that is running this script
    # is located under any of the designated fal environment directories.
    from fal.packages.environments.virtual_env import _BASE_VENV_DIR
    from fal.packages.environments.conda import _BASE_CONDA_DIR

    executable_path = Path(sys.executable)
    for environment_type, prefix in [
        ("venv", _BASE_VENV_DIR),
        ("conda", _BASE_CONDA_DIR),
    ]:
        if prefix in executable_path.parents:
            return environment_type
    else:
        return "local"
