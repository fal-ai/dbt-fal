from __future__ import annotations

import hashlib
import os
import subprocess
import sysconfig
import threading
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, DefaultDict, Iterator, List

from dbt.logger import GLOBAL_LOGGER as logger

from fal.packages import bridge, isolated_runner
from fal.packages.dependency_analysis import get_default_requirements
from fal.packages.environments.base import (
    BASE_CACHE_DIR,
    BaseEnvironment,
    rmdir_on_fail,
)

if TYPE_CHECKING:
    from fal.packages.environments.base import HookRunnerProtocol

_BASE_VENV_DIR = BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(exist_ok=True)


@dataclass(frozen=True)
class VirtualPythonEnvironment(BaseEnvironment):
    requirements: List[str] = field(default_factory=list)

    # TODO: This is not intra-process safe, so if you have 2 running
    # fal processes that tries to create the same virtual environment
    # *at the same time* it might fail (the behavior is undefined)·
    #
    # We might think about introducing a file-lock, which would
    # allow this scenerio in the future.
    _VENV_LOCKS: ClassVar[DefaultDict[str, threading.Lock]] = defaultdict(
        threading.Lock
    )

    @property
    def key(self) -> str:
        return hashlib.sha256(" ".join(self.requirements).encode()).hexdigest()

    def create_or_get_venv(self) -> Path:
        from virtualenv import cli_run

        path = _BASE_VENV_DIR / self.key
        with self._VENV_LOCKS[self.key]:
            with rmdir_on_fail(path):
                if path.exists():
                    return path
                logger.info(f"Creating virtual environment at {path}.")
                cli_run([str(path)])
                logger.info(
                    f"Installing the requirements: {', '.join(self.requirements)}"
                )
                if self.requirements:
                    pip_path = path / "bin" / "pip"
                    subprocess.check_call([pip_path, "install"] + self.requirements)

        return path

    @contextmanager
    def setup(self) -> Iterator[HookRunnerProtocol]:
        primary_env = get_primary_env()
        primary_env_path = primary_env.create_or_get_venv()
        user_env_path = self.create_or_get_venv()
        yield partial(
            self._run, primary_path=primary_env_path, secondary_path=user_env_path
        )

    @contextmanager
    def _prepare_client(
        self,
        service: bridge.Listener,
        primary_path: Path,
        secondary_path: Path,
    ) -> Iterator[bridge.ConnectionWrapper]:
        # We are going to use the primary environment to run the Python
        # interpreter, but at the same time we are going to inherit all
        # the packages from the secondary environment (user's environment)
        # so that they can technically override anything.

        # The search order is important, we want the secondary path to
        # take precedence.
        search_paths = [secondary_path, primary_path]
        python_path = os.pathsep.join(
            # sysconfig takes the virtual environment path and returns
            # the directory where all the site packages are located.
            sysconfig.get_path("purelib", vars={"base": search_path})
            for search_path in search_paths
        )

        python_executable = primary_path / "bin" / "python"
        logger.debug("Starting the process...")
        with subprocess.Popen(
            [
                python_executable,
                isolated_runner.__file__,
                bridge.encode_service_address(service.address),
            ],
            env={"PYTHONPATH": python_path, **os.environ},
        ) as process:
            with service.accept() as connection:
                yield connection


# We manage user-defined virtual-environments in two steps.
#   1. Create a primary environment which contains the default dependencies
#      to run a fal script (like fal, dbt-core, and required dbt adapters).
#   2. Create a secondary environment which contains the user-defined dependencies.
#
# This is an optimization we apply to reduce the cost of user-defined virtual-environments
# where we can actually reuse the primary environment and save a lot of time from not installing
# heavy dependencies like dbt adapters again and again.

_PRIMARY_ENV = None


def get_primary_env() -> VirtualPythonEnvironment:
    global _PRIMARY_ENV
    if not _PRIMARY_ENV:
        _PRIMARY_ENV = VirtualPythonEnvironment(
            [
                f"{key}=={value}" if value else key
                for key, value in get_default_requirements()
            ]
        )
    return _PRIMARY_ENV
