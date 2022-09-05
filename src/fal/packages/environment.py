from __future__ import annotations

import hashlib
import shutil
import subprocess
import threading
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    ContextManager,
    DefaultDict,
    Iterator,
    List,
)

from dbt.logger import GLOBAL_LOGGER as logger
from platformdirs import user_cache_dir

from fal.packages import bridge, isolated_runner
from fal.packages.dependency_analysis import get_default_requirements

_BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
_BASE_CACHE_DIR.mkdir(exist_ok=True)

_BASE_VENV_DIR = _BASE_CACHE_DIR / "venvs"
_BASE_VENV_DIR.mkdir(exist_ok=True)

InternalHook = Callable[[], int]

if TYPE_CHECKING:
    from typing import Protocol

    class HookRunnerProtocol(Protocol):
        def __call__(self, executable: InternalHook, *args, **kwargs) -> int:
            ...


@contextmanager
def _clear_on_fail(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        shutil.rmtree(path)
        raise


class BaseEnvironment:
    def setup(self) -> ContextManager[HookRunnerProtocol]:
        raise NotImplementedError

    def _prepare_client(
        self,
        service: bridge.Listener,
        *args,
        **kwargs,
    ) -> ContextManager[bridge.ConnectionWrapper]:
        raise NotImplementedError

    def _run(self, executable: InternalHook, *args, **kwargs) -> int:
        # The controller here assumes that there will be at most one
        # client. This restriction might change in the future.
        logger.debug("Starting the controller bridge.")
        with bridge.controller_connection() as controller_service:
            logger.debug(
                "Controller connection is established at {}", controller_service.address
            )
            with self._prepare_client(
                controller_service, *args, **kwargs
            ) as connection:
                logger.debug(
                    "Child connection has been established at the bridge {}",
                    controller_service.address,
                )
                # TODO: check_alive() here.
                connection.send(executable)
                logger.debug(
                    "Awaiting the child process to exit at {}",
                    controller_service.address,
                )
                return self._receive_status(connection)

    def _receive_status(self, connection: bridge.ConnectionWrapper) -> int:
        try:
            status, exception = connection.recv()
        except EOFError:
            raise RuntimeError("The child process has unexpectedly exited.")

        if exception is not None:
            logger.error("An exception has occurred: {}", exception)
            raise exception

        logger.debug("Isolated process has exitted with status: {}", status)
        assert status is not None
        return status


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
            for key, value in get_default_requirements()
        )

    @property
    def _key(self) -> str:
        # The key is used to identify a set of dependencies for a
        # given virtual environment when we are dealing with caches.
        #
        # Note that we also sort the requirements to make sure even
        # with a different order, the environments are cached.
        return hashlib.sha256(" ".join(self.requirements).encode()).hexdigest()

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

    @contextmanager
    def setup(self) -> Iterator[HookRunnerProtocol]:
        venv_path = self._create_venv()
        yield partial(self._run, venv_path=venv_path)

    @contextmanager
    def _prepare_client(
        self,
        service: bridge.Listener,
        venv_path: Path,
    ) -> Iterator[bridge.ConnectionWrapper]:
        python_path = venv_path / "bin" / "python"
        logger.debug("Starting the process...")
        with subprocess.Popen(
            [
                python_path,
                isolated_runner.__file__,
                bridge.encode_service_address(service.address),
            ],
        ) as process:
            with service.accept() as connection:
                yield connection


def create_environment(*, requirements: List[str]) -> BaseEnvironment:
    return VirtualPythonEnvironment(requirements=requirements)
