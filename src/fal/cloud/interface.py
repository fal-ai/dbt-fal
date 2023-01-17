from dataclasses import dataclass, replace
from functools import partial, wraps
from typing import Any, Callable, Dict, Optional
from backports.functools_lru_cache import namedtuple

import importlib_metadata
from mashumaro import config
from isolate.backends import BaseEnvironment
from isolate.backends.conda import CondaEnvironment
from isolate.backends.settings import IsolateSettings
from isolate.backends.virtualenv import VirtualPythonEnvironment
from isolate_cloud.api import FalHostedServer


AVAILABLE_TARGETS: Dict[str, BaseEnvironment] = {
    "conda": CondaEnvironment,
    "virtualenv": VirtualPythonEnvironment
}


@dataclass
class BaseHost:
    def build_env(self, target_kind: str, config: Any) -> BaseEnvironment:
        raise NotImplementedError()


@dataclass
class FalCloudHost(BaseHost):
    kind: str = "remote"
    url: str = "34.173.229.191:6005"
    machine_type: str = "XS"

    def build_env(self, target_kind: str, config: Any) -> BaseEnvironment:
        parsed_config = _parse_config(config)
        definition = {
            "kind": target_kind,
            "configuration": parsed_config
        }
        return FalHostedServer.from_config(
            {
                "host": self.url,
                "machine_type": self.machine_type,
                "target_environments": [definition],
            }
        )

@dataclass
class LocalHost(BaseHost):
    def build_env(self, target_kind: str, config: Any) -> BaseEnvironment:
        env_class = AVAILABLE_TARGETS.get(target_kind)
        if not env_class:
            raise NotImplementedError(f"Unknown environment kind: {target_kind}")
        parsed_config = _parse_config(config)
        parsed_config = _add_dill(target_kind, parsed_config)

        settings = IsolateSettings(serialization_method="dill")

        return env_class.from_config(parsed_config, settings=settings)


@dataclass
class IsolatedFunction:
    fn: Callable
    kind: str
    config: Dict
    host: Optional[BaseHost] = None

    def __post_init__(self):
        if not self.host:
            machine_type = self.config.get("machine_type")
            self.host = FalCloudHost(machine_type=machine_type)

    def __call__(self, *args, **kwargs):
        env = self.host.build_env(self.kind, self.config)

        key = env.create()

        with env.open_connection(key) as connection:
            res = connection.run(partial(self.fn, *args, **kwargs))
            return res

    def on(self, host: Optional[BaseHost] = None, **new_config):
        # TODO: validate configs
        if isinstance(host, BaseHost) or host is None:
            new_host_config = {**self.config, **new_config}
        else:
            new_host_config = new_config

        return replace(
            self,
            host=host,
            config=new_host_config,
        )


def isolated(kind: str, **config: Any):
    if kind not in AVAILABLE_TARGETS.keys():
        raise NotImplementedError(f"Unknown environment kind: {kind}")
    def fn_outer(fn):
        return IsolatedFunction(
            kind=kind, fn=fn, config=config)
    return fn_outer


local = LocalHost
cloud = FalCloudHost

def _add_dill(kind: str, config: Dict) -> Dict:
    default_pkgs = []
    serializer = "dill"

    default_pkgs.append(
        (serializer, importlib_metadata.version(serializer))
    )

    if kind == "virtualenv":
        requirements = config.setdefault("requirements", [])
        requirements.extend(f"{name}=={version}" for name, version in default_pkgs)
        return config
    elif kind == "conda":
        packages = config.setdefault("packages", [])
        packages.extend(f"{name}={version}" for name, version in default_pkgs)
        return config
    else:
        raise NotImplementedError(f"Unknown environment kind: {kind}")

def _parse_config(config: Dict) -> Dict:
    CONFIG_KEYS_TO_IGNORE = ['host', 'type', 'name', 'machine_type']
    return { key: val for key, val in config.items() if key not in CONFIG_KEYS_TO_IGNORE}
