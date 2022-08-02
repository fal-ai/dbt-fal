from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import threading
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import cached_property, partial
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    ContextManager,
    DefaultDict,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)

from platformdirs import user_cache_dir

import fal.packages._run_hook as _hook_runner_module
from faldbt.project import FalDbt
from fal.planner.tasks import SUCCESS, FAILURE

_BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
_BASE_CACHE_DIR.mkdir(exist_ok=True)

_BASE_VENV_DIR = _BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(exist_ok=True)

# TODO: for dev versions, we need a better solution.
_IS_DEV_VERSION = True


def _get_default_requirements() -> Iterator[Tuple[str, Optional[str]]]:
    import importlib_metadata

    # TODO: for dev versions, we need a better solution.
    if _IS_DEV_VERSION:
        yield "git+https://github.com/fal-ai/fal@batuhan/fea-318-use-the-environment-option-for-local", None
    else:
        yield "fal", importlib_metadata.version("fal")
    yield "dbt-core", importlib_metadata.version("dbt-core")

    for dynamic_dependency in importlib_metadata.distributions():
        if dynamic_dependency.name.startswith("dbt-"):
            yield dynamic_dependency.name, dynamic_dependency.version


HookRunner = Callable[[FalDbt, Path, str, Dict[str, Any], int], int]


@contextmanager
def _clear_on_fail(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        shutil.rmtree(path)
        raise


class BaseEnvironment:
    def setup(self) -> ContextManager[HookRunner]:
        raise NotImplementedError


@dataclass(frozen=True)
class VirtualPythonEnvironment(BaseEnvironment):
    requirements: List[str] = field(default_factory=list)

    # TODO: This is not intra-process safe, so if you have 2 running
    # fal processes that tries to create the same virtual environment
    # *at the same time* it might fail (the behavior is undefined)·
    #
    # We might think about introducing a file-lock, which would
    # allow this scenerio in the future.
    _VENV_LOCKS: ClassVar[DefaultDict[threading.Lock]] = defaultdict(threading.Lock)

    def __post_init__(self) -> None:
        self.requirements.extend(
            f"{key}=={value}" if value else key
            for key, value in _get_default_requirements()
        )

    @cached_property
    def _key(self) -> str:
        crypt = hashlib.sha256()
        crypt.update(" ".join(self.requirements).encode())
        return crypt.hexdigest()

    @contextmanager
    def setup(self) -> Iterator[HookRunner]:
        venv_path = self._create_venv()
        yield partial(self._run_in_venv, venv_path)

    def _create_venv(self) -> Path:
        from virtualenv import cli_run

        path = _BASE_VENV_DIR / self._key
        with self._VENV_LOCKS[self._key]:
            with _clear_on_fail(path):
                if path.exists():
                    return path

                print(f"Creating virtual environment at {path}.")
                cli_run([str(path)])

                print(f"Installing the requirements: {', '.join(self.requirements)}")
                pip_path = path / "bin" / "pip"
                subprocess.check_call([pip_path, "install"] + self.requirements)

        return path

    def _run_in_venv(
        self,
        venv_path: Path,
        fal_dbt: FalDbt,
        hook_path: Path,
        arguments: Dict[str, Any],
        bound_model_name: str,
        run_index: int,
    ) -> int:
        python_path = venv_path / "bin" / "python"
        data = json.dumps(
            {
                "path": str(hook_path),
                "bound_model_name": bound_model_name,
                "fal_dbt_config": fal_dbt._serialize(),
                "arguments": arguments,
                "run_index": run_index,
            }
        )

        process = subprocess.Popen(
            [
                python_path,
                _hook_runner_module.__file__,
                data,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in process.stdout:
            print(line, end="")
        return SUCCESS if process.wait() == 0 else FAILURE


def create_environment(*, requirements: Optional[List[str]] = None) -> BaseEnvironment:
    return VirtualPythonEnvironment(requirements=requirements)
