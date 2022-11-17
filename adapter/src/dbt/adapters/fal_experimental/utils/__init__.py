from contextlib import contextmanager
from typing import Any

from dbt.config.runtime import RuntimeConfig

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache


FAL_SCRIPTS_PATH_VAR_NAME = 'fal-scripts-path'


def cache_static(func):
    """Cache the result of a function."""
    return lru_cache(maxsize=None)(func)


def retrieve_symbol(source_code: str, symbol_name: str) -> Any:
    """Retrieve the function with the given name from the source code."""
    namespace = {}
    exec(source_code, namespace)
    return namespace[symbol_name]


def get_fal_scripts_path(config: RuntimeConfig):
    import pathlib
    project_path = pathlib.Path(config.project_root)

    # Default value
    fal_scripts_path = ''

    if hasattr(config, 'vars'):
        fal_scripts_path: str = config.vars.to_dict().get(FAL_SCRIPTS_PATH_VAR_NAME, fal_scripts_path)  # type: ignore

    if hasattr(config, 'cli_vars'):
        fal_scripts_path = config.cli_vars.get(FAL_SCRIPTS_PATH_VAR_NAME, fal_scripts_path)

    return project_path / fal_scripts_path


@contextmanager
def extra_path(path: str):
    import sys
    sys.path.append(path)
    try:
        yield
    finally:
        sys.path.remove(path)
