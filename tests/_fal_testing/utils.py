import os
from pathlib import Path
from functools import partial

def create_artifact(context, suffix):
    model_name = context.current_model.name
    model_status = context.current_model.status

    temp_dir = Path(os.environ["temp_dir"])
    temp_file = (temp_dir / model_name).with_suffix(suffix)

    output = f"Model name: {model_name}"
    output += f"\nStatus: {model_status}"
    output += f"\nModel dataframe name: {model_name}"
    temp_file.write_text(output)


def create_model_artifact(context):
    create_artifact(context, ".txt")


def create_script_artifact(context, prompt):
    create_artifact(context, f".{prompt}.txt")


create_before_script_artifact = partial(create_script_artifact, prompt="before")
create_after_script_artifact = partial(create_script_artifact, prompt="after")
create_pre_hook_artifact = partial(create_script_artifact, prompt="pre_hook")
create_post_hook_artifact = partial(create_script_artifact, prompt="post_hook")
