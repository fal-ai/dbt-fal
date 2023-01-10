from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from dbt.events.adapter_endpoint import AdapterLogger

import dbt.exceptions
import importlib_metadata

from isolate.backends import BaseEnvironment, BasicCallable, EnvironmentConnection

from dbt.config.runtime import RuntimeConfig
from isolate.backends.local import LocalPythonEnvironment

from . import cache_static

from .yaml_helper import load_yaml


CONFIG_KEYS_TO_IGNORE = ['host', 'remote_type', 'type', 'name', 'machine_type']
REMOTE_TYPES_DICT = {
    "venv": "virtualenv",
    "conda": "conda"
}

logger = AdapterLogger('fal')

class FalParseError(Exception):
    pass


@dataclass
class LocalConnection(EnvironmentConnection):
    def run(self, executable: BasicCallable, *args, **kwargs) -> Any:
        return executable(*args, **kwargs)


def fetch_environment(
        project_root: str,
        environment_name: str,
        machine_type: str = "S",
        credentials: Optional[Any] = None
) -> Tuple[BaseEnvironment, bool]:
    """Fetch the environment with the given name from the project's
    fal_project.yml file."""
    # Local is a special environment where it doesn't need to be defined
    # since it will mirror user's execution context directly.
    if environment_name == "local":
        if credentials.key_secret and credentials.key_id:
            # "local" environment in this case is fal cloud
            # TODO: add docs link
            logger.warning("`local` environments will be executed on fal cloud." + \
                           "If you don't want to use fal cloud, you can change your " + \
                           "profile target to one where fal doesn't have credentials")
            return _build_hosted_env(
                config={'name': '', 'type': 'venv'},
                parsed_config={},
                credentials=credentials,
                machine_type=machine_type), False

        return LocalPythonEnvironment(), True
    try:
        environments = load_environments(project_root, machine_type, credentials)
    except Exception as exc:
        raise dbt.exceptions.RuntimeException(
            "Error loading environments from fal_project.yml"
        ) from exc

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


def load_environments(
        base_dir: str,
        machine_type: str = "S",
        credentials: Optional[Any] = None) -> Dict[str, BaseEnvironment]:
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
            env_name,
            env_kind,
            environment,
            machine_type,
            credentials)

    return environments


def create_environment(
        name: str,
        kind: str,
        config: Dict[str, Any],
        machine_type: str = "S",
        credentials: Optional[Any] = None):
    from isolate.backends.virtualenv import VirtualPythonEnvironment
    from isolate.backends.conda import CondaEnvironment
    from isolate.backends.remote import IsolateServer

    parsed_config = { key: val for key, val in config.items() if key not in CONFIG_KEYS_TO_IGNORE}

    if credentials.key_secret and credentials.key_id:
        return _build_hosted_env(config, parsed_config, credentials, machine_type)

    REGISTERED_ENVIRONMENTS: Dict[str, BaseEnvironment] = {
        "conda": CondaEnvironment,
        "venv": VirtualPythonEnvironment,
        "remote": IsolateServer,
    }

    env_type = REGISTERED_ENVIRONMENTS.get(kind)

    if env_type is None:
        raise ValueError(
            f"Invalid environment type (of {kind}) for {name}. Please choose from: "
            + ", ".join(REGISTERED_ENVIRONMENTS.keys())
        )

    if kind == "remote":
        parsed_config = _parse_remote_config(config, parsed_config)

    return env_type.from_config(parsed_config)


def _build_hosted_env(
        config: Dict[str, Any],
        parsed_config: Dict[str, Any],
        credentials: Any,
        machine_type: str
) -> BaseEnvironment:
    from isolate_cloud.api import FalHostedServer
    from isolate_cloud.api import CloudKeyCredentials
    if not config.get("type"):
        kind = "virtualenv"
    else:
        if config["type"] not in REMOTE_TYPES_DICT.keys():
            raise RuntimeError(
                f"Environment type {config['type']} is not supported. Supported types: {REMOTE_TYPES_DICT.keys()}")
        kind = REMOTE_TYPES_DICT[config["type"]]

    if kind == "virtualenv":

        if "requirements" not in parsed_config.keys():
            parsed_config["requirements"] = []

        parsed_config["requirements"].append(f"dill=={importlib_metadata.version('dill')}")

    elif kind == "conda":
        if "packages" not in parsed_config.keys():
            parsed_config["packages"] = []

        parsed_config["packages"].append(f"dill={importlib_metadata.version('dill')}")

    env_definition = {
        "kind": kind,
        "configuration": parsed_config
    }

    return FalHostedServer.from_config(
        {
            "host": credentials.host,
            "creds": CloudKeyCredentials(credentials.key_id, credentials.key_secret),
            "machine_type": machine_type,
            "target_environments": [env_definition]
        }
    )


def _is_local_environment(environment_name: str) -> bool:
    return environment_name == "local"


def _get_required_key(data: Dict[str, Any], name: str) -> Any:
    if name not in data:
        raise FalParseError("Missing required key: " + name)
    return data[name]

def _parse_remote_config(config: Dict[str, Any], parsed_config: Dict[str, Any]) -> Dict[str, Any]:
    assert config.get("remote_type"), "remote_type needs to be specified."

    remote_type = REMOTE_TYPES_DICT.get(config["remote_type"])

    assert remote_type, f"{config['remote_type']} not recognised. Available remote types: {list(REMOTE_TYPES_DICT.keys())}"

    env_definition = {
        "kind": remote_type,
        "configuration": parsed_config
    }

    return {
        "host": config.get("host"),
        "target_environments": [env_definition]
    }


def _get_dbt_packages(
    adapter_type: str,
    is_teleport: bool = False,
    is_remote: bool = False
) -> Iterator[Tuple[str, Optional[str]]]:
    dbt_adapter = f"dbt-{adapter_type}"
    for dbt_plugin_name in ['dbt-core', dbt_adapter]:
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

    if _is_pre_release(dbt_fal_version):
        if is_remote:
            # If it's a pre-release and it's remote, its likely us developing, so we try installing
            # from Github and we can get the custom branch name from FAL_GITHUB_BRANCH environment variable
            # TODO: Handle pre-release on PyPI. How should we approach that?
            import os
            branch_name = os.environ.get('FAL_GITHUB_BRANCH', 'main')

            dbt_fal_suffix = f" @ git+https://github.com/fal-ai/fal.git@{branch_name}#subdirectory=adapter"
            dbt_fal_version = None
        else:
            dbt_fal_path = _get_adapter_root_path()
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


def _is_pre_release(raw_version: str) -> bool:
    from dbt.semver import VersionSpecifier
    adapter_version = VersionSpecifier.from_version_string(raw_version)
    return bool(adapter_version.prerelease)


def _get_adapter_root_path() -> Optional[Path]:
    import dbt.adapters.fal as adapter

    base_dir = Path(adapter.__file__).parent.parent.parent.parent.parent
    # TODO: this can happen with REAL pre-releases
    return base_dir if (base_dir.parent / ".git").exists() else None


def get_default_requirements(
    adapter_type: str,
    is_teleport: bool = False,
    is_remote: bool = False
) -> Iterator[Tuple[str, Optional[str]]]:
    yield from _get_dbt_packages(adapter_type, is_teleport, is_remote)
    if not is_remote:
        # We don't override remote isolate
        yield "isolate", importlib_metadata.version("isolate")


@cache_static
def get_default_pip_dependencies(
    adapter_type: str,
    is_teleport: bool = False,
    is_remote: bool = False,
) -> List[str]:
    return [
        f"{package}=={version}" if version else package
        for package, version in get_default_requirements(adapter_type, is_teleport, is_remote)
    ]
