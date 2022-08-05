import os
import inspect
from pathlib import Path


def create_artifact(context, suffix, additional_data=None):
    model = context.current_model
    model_name = model.name if model else "GLOBAL"
    model_status = model.status if model else None

    temp_dir = Path(os.environ["temp_dir"])
    temp_file = (temp_dir / model_name).with_suffix(suffix)

    output = f"Model name: {model_name}"
    output += f"\nStatus: {model_status}"
    output += f"\nModel dataframe name: {model_name}"
    if additional_data:
        output += f"\n{additional_data}"
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
