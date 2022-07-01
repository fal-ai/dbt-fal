import os
from pathlib import Path


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


def create_post_hook_artifact(context, prompt="post_hook"):
    create_artifact(context, f".{prompt}.txt")
