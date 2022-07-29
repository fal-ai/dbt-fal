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


class DynamicIndexProvider:
    def __init__(self) -> None:
        self._index = 0

    def next(self) -> int:
        """Increment the counter and return the last value."""
        index = self._index
        self._index += 1
        return index

    def __int__(self) -> int:
        """Return the last value."""
        return self._index
