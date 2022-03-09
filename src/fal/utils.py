"""Fal utilities."""
from dbt.logger import print_timestamped_line
from fal.fal_script import FalScript
from typing import List


def print_run_info(scripts: List[FalScript]):
    """Print information on the current fal run."""
    models_arr = []
    for script in scripts:
        path = str(script.path)
        models_arr.append(f"{script.model.name}: {path}")

    models_str = "\n".join(models_arr)
    print_timestamped_line(
        f"Starting fal run for following models and scripts: \n{models_str}\n"
    )
