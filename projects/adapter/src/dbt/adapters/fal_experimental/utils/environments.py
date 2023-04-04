from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Tuple
import importlib_metadata

from dbt.events.adapter_endpoint import AdapterLogger
from dbt.exceptions import DbtRuntimeError
from dbt.config.runtime import RuntimeConfig

from isolate.backends import BaseEnvironment, BasicCallable, EnvironmentConnection
from fal_serverless import FalServerlessKeyCredentials, LocalHost
from fal_serverless.api import Host, FalServerlessHost

from . import cache_static
from .yaml_helper import load_yaml


CONFIG_KEYS_TO_IGNORE = ["host", "remote_type", "type", "name", "machine_type"]
REMOTE_TYPES_DICT = {
    "venv": "virtualenv",
    "conda": "conda",
}

logger = AdapterLogger("fal")


class FalParseError(Exception):
    pass


@dataclass
class LocalConnection(EnvironmentConnection):
    def run(self, executable: BasicCallable, *args, **kwargs) -> Any:
        return executable(*args, **kwargs)


@dataclass
class EnvironmentDefinition():
    host: Host
    kind: str
    config: dict[Any, Any]
    machine_type: str = "S"


def fetch_environment(
    project_root: str,
    environment_name: str,
    machine_type: str = "S",
    credentials: Optional[Any] = None,
) -> Tuple[EnvironmentDefinition, bool]:
    """Fetch the environment with the given name from the project's
    fal_project.yml file."""
    # Local is a special environment where it doesn't need to be defined
    # since it will mirror user's execution context directly.
    if environment_name == "local":
        if credentials.host:
            logger.warning(
                "`local` environments will be executed on fal cloud."
                + "If you don't want to use fal cloud, you can change your "
                + "profile target to one where fal doesn't have credentials"
            )
            host = FalServerlessHost(
                url=credentials.host,
                credentials=FalServerlessKeyCredentials(credentials.key_id, credentials.key_secret))
            return EnvironmentDefinition(
                host=host,
                kind="virtualenv",
                machine_type=machine_type,
                config={"name": "", "type": "venv"}), False

        return EnvironmentDefinition(host=LocalHost(), kind="local", config={}), True

    try:
        environments = load_environments(project_root, machine_type, credentials)
    except Exception as exc:
        raise DbtRuntimeError(
            "Error loading environments from fal_project.yml"
        ) from exc

    if environment_name not in environments:
        raise DbtRuntimeError(
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


def load_environments(
    base_dir: str, machine_type: str = "S", credentials: Optional[Any] = None
) -> Dict[str, EnvironmentDefinition]:
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
        if environments.get(env_name) is not None:
            raise FalParseError("Environment names must be unique.")

        environments[env_name] = create_environment(
            env_name, env_kind, environment, machine_type, credentials
        )

    return environments


def create_environment(
    name: str,
    kind: str,
    config: Dict[str, Any],
    machine_type: str = "S",
    credentials: Optional[Any] = None,
) -> EnvironmentDefinition:
    if kind not in ["venv", "conda"]:
        raise ValueError(
            f"Invalid environment type (of {kind}) for {name}. Please choose from: "
            + "venv, conda."
        )

    kind = kind if kind == "conda" else "virtualenv"

    parsed_config = {
        key: val for key, val in config.items() if key not in CONFIG_KEYS_TO_IGNORE
    }

    if credentials.key_secret and credentials.key_id:
        host = FalServerlessHost(
            url=credentials.host,
            credentials=FalServerlessKeyCredentials(credentials.key_id, credentials.key_secret))
    else:
        host = LocalHost()
    return EnvironmentDefinition(
        host=host,
        kind=kind,
        config=parsed_config,
        machine_type=machine_type)


def _is_local_environment(environment_name: str) -> bool:
    return environment_name == "local"


def _get_required_key(data: Dict[str, Any], name: str) -> Any:
    if name not in data:
        raise FalParseError("Missing required key: " + name)
    return data[name]

def _parse_remote_config(
    config: Dict[str, Any], parsed_config: Dict[str, Any]
) -> Dict[str, Any]:
    assert config.get("remote_type"), "remote_type needs to be specified."

    remote_type = REMOTE_TYPES_DICT.get(config["remote_type"])

    assert (
        remote_type
    ), f"{config['remote_type']} not recognised. Available remote types: {list(REMOTE_TYPES_DICT.keys())}"

    env_definition = {
        "kind": remote_type,
        "configuration": parsed_config,
    }

    return {
        "host": config.get("host"),
        "target_environments": [env_definition],
    }

def _get_package_from_type(adapter_type: str):
    SPECIAL_ADAPTERS = {
        # Documented in https://docs.getdbt.com/docs/supported-data-platforms#community-adapters
        "athena": "dbt-athena-community",
    }
    return SPECIAL_ADAPTERS.get(adapter_type, f"dbt-{adapter_type}")

def _get_dbt_packages(
    adapter_type: str,
    is_teleport: bool = False,
    is_remote: bool = False,
) -> Iterator[Tuple[str, Optional[str]]]:
    dbt_adapter = _get_package_from_type(adapter_type)
    for dbt_plugin_name in [dbt_adapter]:
        distribution = importlib_metadata.distribution(dbt_plugin_name)

        yield dbt_plugin_name, distribution.version

    try:
        dbt_fal_version = importlib_metadata.version("dbt-fal")
    except importlib_metadata.PackageNotFoundError:
        # It might not be installed.
        return None

    dbt_fal_dep = "dbt-fal"
    dbt_fal_extras = _find_adapter_extras(dbt_fal_dep, dbt_adapter)
    if is_teleport:
        dbt_fal_extras.add("teleport")
    dbt_fal_suffix = ""

    if _version_is_prerelease(dbt_fal_version):
        if is_remote:
            # If it's a pre-release and it's remote, its likely us developing, so we try installing
            # from Github and we can get the custom branch name from FAL_GITHUB_BRANCH environment variable
            # TODO: Handle pre-release on PyPI. How should we approach that?
            import os

            branch_name = os.environ.get("FAL_GITHUB_BRANCH", "main")

            dbt_fal_suffix = f" @ git+https://github.com/fal-ai/fal.git@{branch_name}#subdirectory=projects/adapter"
            dbt_fal_version = None
        else:
            dbt_fal_path = _get_project_root_path("adapter")
            if dbt_fal_path is not None:
                # Can be a pre-release from PyPI
                dbt_fal_dep = str(dbt_fal_path)
                dbt_fal_version = None

    dbt_fal = f"{dbt_fal_dep}[{' ,'.join(dbt_fal_extras)}]{dbt_fal_suffix}"
    yield dbt_fal, dbt_fal_version


def _find_adapter_extras(package: str, plugin_package: str) -> set[str]:
    import pkgutil
    import dbt.adapters

    all_extras = _get_extras(package)
    available_plugins = {
        module_info.name
        for module_info in pkgutil.iter_modules(dbt.adapters.__path__)
        if module_info.ispkg and module_info.name in plugin_package
    }
    return available_plugins.intersection(all_extras)


def _get_extras(package: str) -> list[str]:
    import importlib_metadata

    dist = importlib_metadata.distribution(package)
    return dist.metadata.get_all("Provides-Extra", [])


def _version_is_prerelease(raw_version: str) -> bool:
    from packaging.version import Version

    package_version = Version(raw_version)
    return package_version.is_prerelease


def _get_project_root_path(package: str) -> Path:
    from dbt.adapters import fal

    # If this is a development version, we'll install
    # the current fal itself.
    path = Path(fal.__file__)
    while path is not None:
        if (path.parent / ".git").exists():
            break
        path = path.parent
    return path / package


def get_default_requirements(
    adapter_type: str,
    is_teleport: bool = False,
    is_remote: bool = False,
) -> Iterator[Tuple[str, Optional[str]]]:
    yield from _get_dbt_packages(adapter_type, is_teleport, is_remote)

@cache_static
def get_default_pip_dependencies(
    adapter_type: str,
    is_teleport: bool = False,
    is_remote: bool = False,
) -> List[str]:
    return [
        f"{package}=={version}" if version else package
        for package, version in get_default_requirements(
            adapter_type, is_teleport, is_remote
        )
    ]
