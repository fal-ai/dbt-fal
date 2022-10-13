"""Fal utilities."""
import copy
from faldbt.logger import LOGGER
from typing import List, TYPE_CHECKING


try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

if TYPE_CHECKING:
    from fal.fal_script import FalScript


def print_run_info(scripts: List["FalScript"]):
    """Print information on the current fal run."""
    models_str = "\n".join(map(lambda script: script.id, scripts))
    LOGGER.info(f"Starting fal run for following models and scripts: \n{models_str}\n")


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


class _ReInitialize:
    def __init__(self, *args, **kwargs):
        self._serialization_state = {
            "args": copy.deepcopy(args),
            "kwargs": copy.deepcopy(kwargs),
        }
        super().__init__(*args, **kwargs)

    def __getstate__(self):
        return self._serialization_state

    def __setstate__(self, state):
        super().__init__(*state["args"], **state["kwargs"])


def has_side_effects(cls):
    """The given class has possible side-effects that might
    make the regular serialization problematic (e.g. registering
    adapters to DBT's global factory)."""
    return type(cls.__name__, (_ReInitialize, cls), {})


def cache_static(func):
    """Cache the result of a function."""
    return lru_cache(maxsize=None)(func)
