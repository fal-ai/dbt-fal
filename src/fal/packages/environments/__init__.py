from typing import List

from fal.packages.environments.base import BASE_CACHE_DIR, BaseEnvironment
from fal.packages.environments.virtual_env import VirtualPythonEnvironment


def create_environment(*, requirements: List[str]) -> BaseEnvironment:
    return VirtualPythonEnvironment(requirements=requirements)
