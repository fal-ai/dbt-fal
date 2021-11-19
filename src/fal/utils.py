"""Fal utilities."""
from dbt.logger import print_timestamped_line
from faldbt.project import DbtModel
from typing import List


def print_run_info(models: List[DbtModel]):
    """Print information on the current fal run."""
    models_arr = map(
        lambda model: f"{model.name}: {', '.join(model.meta['fal']['scripts'])}",
        models
    )
    models_str = '\n'.join(models_arr)
    print_timestamped_line(f"Starting FAL run for following models (model_name: scripts): \n{models_str}")
