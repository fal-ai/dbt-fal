"""Fal utilities."""
from dbt.logger import print_timestamped_line
from fal.fal_script import FalScript
from typing import List


def print_run_info(scripts: List[FalScript]):
    """Print information on the current fal run."""
    models_str = "\n".join(map(lambda script: script.id, scripts))
    print_timestamped_line(
        f"Starting fal run for following models and scripts: \n{models_str}\n"
    )
