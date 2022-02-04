"""Fal utilities."""
from dataclasses import dataclass
from dbt.logger import print_timestamped_line, GLOBAL_LOGGER as logger
from typing import List, Any


@dataclass
class FalLogger:
    disabled: bool

    def print_run_info(self, models: List[Any], keyword: str):
        """Print information on the current fal run."""
        if self.disabled:
            return
        models_arr = []
        for model in models:
            models_arr.append(f"{model.name}: {', '.join(model.get_scripts(keyword))}")

        models_str = "\n".join(models_arr)
        print_timestamped_line(
            f"Starting fal run for following models and scripts: \n{models_str}\n"
        )

    def warn(self, *args, **kwargs):
        if not self.disabled:
            logger.warn(*args, **kwargs)

    def info(self, *args, **kwargs):
        if not self.disabled:
            logger.info(*args, **kwargs)

    def re_enable(self):
        if not self.disabled and logger.disabled:
            logger.enable()
