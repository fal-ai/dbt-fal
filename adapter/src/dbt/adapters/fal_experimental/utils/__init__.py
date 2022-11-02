from typing import Any

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache


def cache_static(func):
    """Cache the result of a function."""
    return lru_cache(maxsize=None)(func)


def retrieve_symbol(source_code: str, symbol_name: str) -> Any:
    """Retrieve the function with the given name from the source code."""
    namespace = {}
    exec(source_code, namespace)
    return namespace[symbol_name]

