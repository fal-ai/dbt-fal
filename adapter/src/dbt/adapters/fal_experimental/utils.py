from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dbt.exceptions
import importlib_metadata

from isolate.backends import BaseEnvironment, BasicCallable, EnvironmentConnection

from dbt.config.runtime import RuntimeConfig

from .yaml_helper import load_yaml

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

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
class LocalConnection(EnvironmentConnection):
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

def cache_static(func):
    """Cache the result of a function."""
    return lru_cache(maxsize=None)(func)

def _get_dbt_packages() -> Iterator[Tuple[str, Optional[str]]]:
    # package_distributions will return a mapping of top-level package names to a list of distribution names (
    # the PyPI names instead of the import names). An example distirbution info is the following, which
    # contains both the main exporter of the top-level name (dbt-core) as well as all the packages that
    # export anything to that namespace:
    #   {"dbt": ["dbt-core", "dbt-postgres", "dbt-athena-adapter"]}
    #
    # This won't only include dbt.adapters.xxx, but anything that might export anything to the dbt namespace
    # (e.g. a hypothetical plugin that only exports stuff to dbt.includes.xxx) which in theory would allow us
    # to replicate the exact environment.
    package_distributions = importlib_metadata.packages_distributions()
    for dbt_plugin_name in package_distributions.get("dbt", []):
        distribution = importlib_metadata.distribution(dbt_plugin_name)

        # Handle dbt-fal separately (since it needs to be installed
        # with its extras).
        if dbt_plugin_name == "dbt-fal":
            continue

        yield dbt_plugin_name, distribution.version

    try:
        dbt_fal_version = importlib_metadata.version("dbt-fal")
    except importlib_metadata.PackageNotFoundError:
        # It might not be installed.
        return None

    if _is_adapter_pre_release():
        dbt_fal_dep = str(_get_adapter_root_path())
        dbt_fal_version = None
    else:
        dbt_fal_dep = "dbt-fal"

    dbt_fal_extras = _find_adapter_extras("dbt-fal")

    if dbt_fal_extras:
        dbt_fal_dep += f"[{' ,'.join(dbt_fal_extras)}]"

    yield dbt_fal_dep, dbt_fal_version


def _find_adapter_extras(package: str) -> Iterator[str]:
    # Return a possible set of extras that might be required when installing
    # adapter in the new environment. The original form which the user has installed
    # is not present to us (it is not saved anywhere during the package installation
    # process, so there is no way for us to know how a user initially installed fal adapter).
    # We'll therefore be as generous as possible and install all the extras for all
    # the dbt.adapters that the user currently has (so this will still be a subset
    # of dependencies, e.g. if there is no dbt.adapter.duckdb then we won't include
    # duckdb as an extra).

    import pkgutil

    import dbt.adapters

    dist = importlib_metadata.distribution(package)
    all_extras = dist.metadata.get_all("Provides-Extra", [])

    # This list is different from the one we obtain in _get_dbt_packages
    # since the names here are the actual import names, not the PyPI names
    # (e.g. this one will say athena, and the other one will say dbt-athena-adapter).
    available_dbt_adapters = {
        module_info.name
        for module_info in pkgutil.iter_modules(dbt.adapters.__path__)
        if module_info.ispkg
    }

    # There will be adapters which we won't have an extra for (e.g. oraceledb)
    # and there will be extras which the user did not install the adapter for
    # (e.g. dbt-redshift). We want to take the intersection of all the adapters
    # that the user has installed and all the extras that fal provides and find
    # the smallest possible subset of extras that we can install.
    return available_dbt_adapters.intersection(all_extras)



def _is_adapter_pre_release() -> bool:
    from dbt.semver import VersionSpecifier
    raw_adapter_version = importlib_metadata.version('dbt-fal')
    adapter_version = VersionSpecifier.from_version_string(raw_adapter_version)
    return adapter_version.prerelease


def _get_adapter_root_path() -> Path:
    import dbt.adapters.fal as adapter

    base_dir = Path(adapter.__file__).parent.parent.parent.parent.parent
    assert (base_dir.parent / ".git").exists()
    return base_dir


def get_default_requirements() -> Iterator[Tuple[str, Optional[str]]]:
    yield from _get_dbt_packages()
    yield "isolate", importlib_metadata.version("isolate")


@cache_static
def get_default_pip_dependencies() -> List[str]:
    return [
        f"{package}=={version}" if version else package
        for package, version in get_default_requirements()
    ]
