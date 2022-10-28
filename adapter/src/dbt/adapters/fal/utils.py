from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Tuple

import dbt.exceptions
from fal.packages.environments.base import (
    BaseEnvironment,
    BasicCallable,
    EnvironmentConnection,
)
from faldbt.parse import load_environments
from dbt.config.runtime import RuntimeConfig


@dataclass
class LocalEnvironment(BaseEnvironment[None]):
    """A dummy environment for Fal to run the given executable
    locally."""

    @property
    def key(self) -> str:
        return "local"

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> BaseEnvironment:
        return cls()

    def _get_or_create(self) -> None:
        return None

    def open_connection(self, conn_info: None) -> LocalConnection:
        return LocalConnection(self)


@dataclass
class LocalConnection(EnvironmentConnection[LocalEnvironment]):
    def run(self, executable: BasicCallable, *args, **kwargs) -> Any:
        return executable(*args, **kwargs)


def retrieve_symbol(source_code: str, symbol_name: str) -> Any:
    """Retrieve the function with the given name from the source code."""
    namespace = {}
    exec(source_code, namespace)
    return namespace[symbol_name]


def fetch_environment(
    project_root: str, environment_name: str
) -> Tuple[BaseEnvironment, bool]:
    """Fetch the environment with the given name from the project's
    fal_project.yml file."""
    # Local is a special environment where it doesn't need to be defined
    # since it will mirror user's execution context directly.
    if environment_name == "local":
        return LocalEnvironment(), True

    try:
        environments = load_environments(project_root)
    except Exception as exc:
        raise dbt.exceptions.RuntimeException(str(exc)) from exc

    if environment_name not in environments:
        raise dbt.exceptions.RuntimeException(
            f"Environment '{environment_name}' was used but not defined in fal_project.yml"
        )

    return environments[environment_name], False


def db_adapter_config(config: RuntimeConfig) -> RuntimeConfig:
    """Return a config object that has the database adapter as its primary. Only
    applicable when the underlying db adapter is encapsulated."""
    if hasattr(config, "sql_adapter_credentials"):
        new_config = replace(config, credentials=config.sql_adapter_credentials)
        new_config.python_adapter_credentials = config.credentials
    else:
        new_config = config

    return new_config
