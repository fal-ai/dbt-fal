from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Tuple

import dbt.exceptions

from isolate.backends import BaseEnvironment, BasicCallable, EnvironmentConnection

from dbt.config.runtime import RuntimeConfig

from .yaml_helper import load_yaml

class FalParseError(Exception):
    pass


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


def load_environments(base_dir: str) -> Dict[str, BaseEnvironment]:
    import os
    fal_project_path = os.path.join(base_dir, "fal_project.yml")
    if not os.path.exists(fal_project_path):
        raise FalParseError(f"{fal_project_path} must exist to define environments")

    fal_project = load_yaml(fal_project_path)
    environments = {}
    for environment in fal_project.get("environments", []):
        env_name = _get_required_key(environment, "name")
        if _is_local_environment(env_name):
            raise FalParseError(
                f"Environment name conflicts with a reserved name: {env_name}."
            )

        env_kind = _get_required_key(environment, "type")
        environments[env_name] = create_environment(env_name, env_kind, environment)
    return environments


def create_environment(name: str, kind: str, config: Dict[str, Any]):
    from isolate.backends.virtualenv import VirtualPythonEnvironment
    from isolate.backends.conda import CondaEnvironment

    REGISTERED_ENVIRONMENTS: Dict[str, BaseEnvironment] = {
        "conda": CondaEnvironment,
        "venv": VirtualPythonEnvironment,
    }

    env_type = REGISTERED_ENVIRONMENTS.get(kind)

    if env_type is None:
        raise ValueError(
            f"Invalid environment type (of {kind}) for {name}. Please choose from: "
            + ", ".join(REGISTERED_ENVIRONMENTS.keys())
        )

    parsed_config = {
        'requirements': config.get('requirements', [])
    }

    return env_type.from_config(parsed_config)


def _is_local_environment(environment_name: str) -> bool:
    return environment_name == "local"

def _get_required_key(data: Dict[str, Any], name: str) -> Any:
    if name not in data:
        raise FalParseError("Missing required key: " + name)
    return data[name]
