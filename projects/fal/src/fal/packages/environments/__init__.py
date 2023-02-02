from pathlib import Path
from typing import List, Optional, Dict, Any

from fal.packages.environments.base import BASE_CACHE_DIR, BaseEnvironment
from fal.packages.environments.conda import CondaEnvironment
from fal.packages.environments.virtual_env import VirtualPythonEnvironment

REGISTERED_ENVIRONMENTS: Dict[str, BaseEnvironment] = {
    "conda": CondaEnvironment,
    "venv": VirtualPythonEnvironment,
}


def create_environment(name: str, kind: str, config: Dict[str, Any]) -> BaseEnvironment:
    env_type = REGISTERED_ENVIRONMENTS.get(kind)
    if env_type is None:
        raise ValueError(
            f"Invalid environment type (of {kind}) for {name}. Please choose from: "
            + ", ".join(REGISTERED_ENVIRONMENTS.keys())
        )

    return env_type.from_config(config)
