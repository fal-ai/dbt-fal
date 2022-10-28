from __future__ import annotations

import sys
import hashlib
import os
import shutil
import subprocess
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any

from fal.packages.environments.base import (
    BASE_CACHE_DIR,
    BaseEnvironment,
    DualPythonIPC,
    log_env,
    rmdir_on_fail,
)
from fal.packages.environments.virtual_env import get_primary_virtual_env
from fal.utils import cache_static

_BASE_CONDA_DIR = BASE_CACHE_DIR / "conda"
_BASE_CONDA_DIR.mkdir(exist_ok=True)

# Specify the path where the conda binary might
# reside in (or mamba, if it is the preferred one).
_CONDA_COMMAND = os.environ.get("CONDA_EXE", "conda")
_FAL_CONDA_HOME = os.getenv("FAL_CONDA_HOME")


@dataclass
class CondaEnvironment(BaseEnvironment[Path], make_thread_safe=True):
    packages: List[str]
    inherit_from_local: bool = False

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> CondaEnvironment:
        user_provided_packages = config.get("packages", [])
        # TODO: remove this once cross-Python-version serialization is
        # working.
        for raw_requirement in user_provided_packages:
            raw_requirement = raw_requirement.replace(" ", "")
            if raw_requirement.startswith("python"):
                continue

            # Ensure that the package is not python-something but rather
            # python followed by any of the version constraints.
            version_identifier = raw_requirement[len("python") :]
            if version_identifier and version_identifier[0] in (
                "=",
                "<",
                ">",
                "!",
            ):
                raise RuntimeError(
                    "Conda environments cannot customize their Python version."
                )

        # We currently don't support sending/receiving data between
        # different Python versions so we need to make sure that the
        # conda environment that is created also uses the same Python version.
        python_version = sysconfig.get_python_version()
        final_packages = user_provided_packages + [f"python={python_version}"]

        inherit_from_local = config.get("_inherit_from_local", False)
        return cls(final_packages, inherit_from_local=inherit_from_local)

    @property
    def key(self) -> str:
        return hashlib.sha256(" ".join(self.packages).encode()).hexdigest()

    def _get_or_create(self) -> Path:
        env_path = _BASE_CONDA_DIR / self.key
        if env_path.exists():
            return env_path

        with rmdir_on_fail(env_path):
            self._run_conda(
                "create",
                "--yes",
                # The environment will be created under $BASE_CACHE_DIR/conda
                # so that in the future we can reuse it.
                "--prefix",
                env_path,
                *self.packages,
            )
        return env_path

    def _run_conda(self, *args, **kwargs) -> None:
        log_env(self, "Installing conda environment")
        conda_executable = get_conda_executable()
        subprocess.check_call([conda_executable, *args], **kwargs, text=True)

    def open_connection(self, conn_info: Path) -> DualPythonIPC:
        if self.inherit_from_local:
            # Instead of creating a separate environment that only has
            # the same versions of fal/dbt-core etc. you have locally,
            # we can also use your environment as the primary. This is
            # mainly for the development time where the fal or dbt-core
            # you are using is not available on PyPI yet.
            primary_env_path = Path(sys.exec_prefix)
        else:
            primary_env = get_primary_virtual_env()
            primary_env_path = primary_env.get_or_create()
        secondary_env_path = conn_info
        return DualPythonIPC(self, primary_env_path, secondary_env_path)


@cache_static
def get_conda_executable() -> Path:
    for path in [_FAL_CONDA_HOME, None]:
        conda_path = shutil.which(_CONDA_COMMAND, path=path)
        if conda_path is not None:
            return conda_path
    else:
        # TODO: we should probably point to the instructions on how you
        # can install conda here.
        raise RuntimeError(
            "Could not find conda executable. Please install conda or set FAL_CONDA_HOME."
        )
