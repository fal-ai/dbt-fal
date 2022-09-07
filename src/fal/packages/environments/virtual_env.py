from __future__ import annotations

import hashlib
import os
import subprocess
import sysconfig
from dataclasses import dataclass, field
from pathlib import Path
from typing import ContextManager, List

from fal.packages import bridge, isolated_runner
from fal.packages.dependency_analysis import get_default_requirements
from fal.packages.environments.base import (
    BASE_CACHE_DIR,
    BaseEnvironment,
    IsolatedProcessConnection,
    log_env,
    rmdir_on_fail,
)

_BASE_VENV_DIR = BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(exist_ok=True)


@dataclass
class VirtualPythonEnvironment(BaseEnvironment[Path], make_thread_safe=False):
    requirements: List[str] = field(default_factory=list)

    @property
    def key(self) -> str:
        return hashlib.sha256(" ".join(self.requirements).encode()).hexdigest()

    def _python_path_for(self, *search_paths) -> str:
        assert len(search_paths) >= 1
        return os.pathsep.join(
            # sysconfig takes the virtual environment path and returns
            # the directory where all the site packages are located.
            sysconfig.get_path("purelib", vars={"base": search_path})
            for search_path in search_paths
        )

    def _executable_in(self, search_path: Path, executable_name: str) -> Path:
        return search_path / "bin" / executable_name

    def _verify_dependencies(self, primary_path: Path, secondary_path: Path) -> None:
        # Ensure that there are no dependency mismatches between the
        # primary environment and the secondary environment.
        python_path = self._python_path_for(secondary_path, primary_path)
        original_pip = self._executable_in(primary_path, "pip")
        subprocess.check_call([original_pip, "check"], env={"PYTHONPATH": python_path})

    def _get_or_create(self) -> Path:
        from virtualenv import cli_run

        path = _BASE_VENV_DIR / self.key
        if path.exists():
            return path

        with rmdir_on_fail(path):
            log_env(self, "Creating virtual environment at {}", path, kind="info")
            cli_run([str(path)])
            log_env(
                self,
                "Installing requirements: {}",
                ", ".join(self.requirements),
                kind="info",
            )
            if self.requirements:
                pip_path = path / "bin" / "pip"
                subprocess.check_call([pip_path, "install"] + self.requirements)

            primary_env = get_primary_env()
            if self is not primary_env:
                self._verify_dependencies(primary_env._get_or_create(), path)

        return path

    def open_connection(self, conn_info: Path) -> VenvConnection:
        primary_venv = get_primary_env()
        primary_venv_path = primary_venv.get_or_create()
        secondary_venv_path = conn_info
        return VenvConnection(self, primary_venv_path, secondary_venv_path)


@dataclass
class VenvConnection(IsolatedProcessConnection[VirtualPythonEnvironment]):
    primary_path: Path
    secondary_path: Path

    def start_process(
        self,
        service: bridge.Listener,
        *args,
        **kwargs,
    ) -> ContextManager[subprocess.Popen]:
        # We are going to use the primary environment to run the Python
        # interpreter, but at the same time we are going to inherit all
        # the packages from the secondary environment (user's environment)
        # so that they can technically override anything.

        # The search order is important, we want the secondary path to
        # take precedence.
        python_path = self.env._python_path_for(self.secondary_path, self.primary_path)
        python_executable = self.env._executable_in(self.primary_path, "python")
        return subprocess.Popen(
            [
                python_executable,
                isolated_runner.__file__,
                bridge.encode_service_address(service.address),
            ],
            env={"PYTHONPATH": python_path, **os.environ},
        )


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
