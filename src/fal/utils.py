"""Fal utilities."""
from dbt.logger import print_timestamped_line
from faldbt.project import DbtModel
from typing import List


def print_run_info(models: List[DbtModel], keyword: str):
    """Print information on the current fal run."""
    models_arr = []
    for model in models:
        models_arr.append(f"{model.name}: {', '.join(model.get_scripts(keyword))}")

    models_str = "\n".join(models_arr)
    print_timestamped_line(
        f"Starting fal run for following models and scripts: \n{models_str}\n"
    )
