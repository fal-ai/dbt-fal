from __future__ import annotations

import os
import glob
import hashlib
import json
import shutil
import subprocess
import threading
import importlib
import importlib.util
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
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
from dbt.logger import GLOBAL_LOGGER as logger

_BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
_BASE_CACHE_DIR.mkdir(exist_ok=True)

_BASE_VENV_DIR = _BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(exist_ok=True)


# NOTE: This is from dbt https://github.com/dbt-labs/dbt-core/blob/7bd861a3514f70b64d5a6c642b4204b50d0d3f7e/core/dbt/version.py#L209-L236
def _get_dbt_plugins_info() -> Iterator[Tuple[str, str]]:
    for plugin_name in _get_adapter_plugin_names():
        if plugin_name == "core":
            continue
        try:
            mod = importlib.import_module(f"dbt.adapters.{plugin_name}.__version__")
        except ImportError:
            # not an adapter
            continue
        yield plugin_name, mod.version  # type: ignore


def _get_adapter_plugin_names() -> Iterator[str]:
    spec = importlib.util.find_spec("dbt.adapters")
    # If None, then nothing provides an importable 'dbt.adapters', so we will
    # not be reporting plugin versions today
    if spec is None or spec.submodule_search_locations is None:
        return

    for adapters_path in spec.submodule_search_locations:
        version_glob = os.path.join(adapters_path, "*", "__version__.py")
        for version_path in glob.glob(version_glob):
            # the path is like .../dbt/adapters/{plugin_name}/__version__.py
            # except it could be \\ on windows!
            plugin_root, _ = os.path.split(version_path)
            _, plugin_name = os.path.split(plugin_root)
            yield plugin_name


def _get_default_requirements() -> Iterator[Tuple[str, Optional[str]]]:
    import pkg_resources
    import dbt.version
    from dbt.semver import VersionSpecifier

    raw_fal_version = pkg_resources.get_distribution("fal").version
    fal_version = VersionSpecifier.from_version_string(raw_fal_version)
    if fal_version.prerelease:
        import fal

        # If this is a development version, we'll install
        # the current fal itself.
        base_dir = Path(fal.__file__).parent.parent.parent
        assert (base_dir / ".git").exists()
        yield str(base_dir), None
    else:
        yield "fal", raw_fal_version

    yield "dbt-core", dbt.version.__version__

    for adapter_name, adapter_version in _get_dbt_plugins_info():
        yield f"dbt-{adapter_name}", adapter_version


HookRunner = Callable[[FalDbt, Path, str, Dict[str, Any], int, bool], int]


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
    _VENV_LOCKS: ClassVar[DefaultDict[str, threading.Lock]] = defaultdict(
        threading.Lock
    )

    def __post_init__(self) -> None:
        self.requirements.extend(
            f"{key}=={value}" if value else key
            for key, value in _get_default_requirements()
        )

    @property
    def _key(self) -> str:
        # The key is used to identify a set of dependencies for a
        # given virtual environment when we are dealing with caches.
        #
        # Note that we also sort the requirements to make sure even
        # with a different order, the environments are cached.
        return hashlib.sha256(" ".join(self.requirements).encode()).hexdigest()

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

                logger.info(f"Creating virtual environment at {path}.")
                cli_run([str(path)])

                logger.info(
                    f"Installing the requirements: {', '.join(self.requirements)}"
                )
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
        disable_logging: bool,
    ) -> int:
        python_path = venv_path / "bin" / "python"
        data = json.dumps(
            {
                "path": str(hook_path),
                "bound_model_name": bound_model_name,
                "fal_dbt_config": fal_dbt._serialize(),
                "arguments": arguments,
                "run_index": run_index,
                "disable_logging": disable_logging,
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
            logger.info(line, end="")
        return SUCCESS if process.wait() == 0 else FAILURE


def create_environment(*, requirements: List[str]) -> BaseEnvironment:
    return VirtualPythonEnvironment(requirements=requirements)
