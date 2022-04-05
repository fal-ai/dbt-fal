"""Fal utilities."""
from dbt.logger import print_timestamped_line
from fal.fal_script import FalScript
from typing import List


def print_run_info(scripts: List[FalScript]):
    """Print information on the current fal run."""
    models_arr = []
    for script in scripts:
        path = str(script.path)
        model_name = script.model.name if script.model else "GLOBAL"
        models_arr.append(f"{model_name}: {path}")

    models_str = "\n".join(models_arr)
    print_timestamped_line(
        f"Starting fal run for following models and scripts: \n{models_str}\n"
    )
