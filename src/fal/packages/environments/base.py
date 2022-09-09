from __future__ import annotations

import shutil
import subprocess
import threading
from collections import defaultdict
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Callable,
    ContextManager,
    Generic,
    Iterator,
    TypeVar,
)

from fal.logger import LOGGER
from platformdirs import user_cache_dir

from fal.packages import bridge

BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
BASE_CACHE_DIR.mkdir(exist_ok=True)

T = TypeVar("T")
K = TypeVar("K", bound="BaseEnvironment")
BasicCallable = Callable[[], int]


@contextmanager
def rmdir_on_fail(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        shutil.rmtree(path)
        raise


def log_env(env: BaseEnvironment, message: str, *args, kind: str = "trace", **kwargs):
    message = f"[{env.key}] {message}"
    log_method = getattr(LOGGER, kind)
    log_method(message, *args, **kwargs)


class BaseEnvironment(Generic[T]):
    def __init_subclass__(cls, make_thread_safe: bool = False) -> None:
        if make_thread_safe:
            lock_cls = threading.Lock
        else:
            lock_cls = nullcontext

        cls.lock_manager = defaultdict(lock_cls)
        return cls

    @property
    def key(self) -> str:
        """A unique identifier for this environment (combination of requirements,
        python version and other relevant information) that can be used for caching
        and identification purposes."""
        raise NotImplementedError

    def _get_or_create(self) -> T:
        """Implementation of the environment creation and retrieval behavior. Not
        thread safe."""
        raise NotImplementedError

    def get_or_create(self) -> T:
        """If the environment exists, return the connection info for it. If not,
        setup the environment and return it first and then return the newly constructed
        information. Thread safe."""
        with self.lock_manager[self.key]:
            return self._get_or_create()

    def open_connection(self, conn_info: T) -> EnvironmentConnection:
        """Return a new connection to the environment residing inside
        given path."""
        raise NotImplementedError

    @contextmanager
    def connect(self) -> Iterator[EnvironmentConnection]:
        env_info = self.get_or_create()
        with self.open_connection(env_info) as connection:
            yield connection


@dataclass
class EnvironmentConnection(Generic[K]):
    env: K

    def __enter__(self) -> EnvironmentConnection:
        return self

    def __exit__(self, *exc_info):
        return None

    def run(self, executable: BasicCallable, *args, **kwargs) -> Any:
        raise NotImplementedError


@dataclass
class IsolatedProcessConnection(EnvironmentConnection[K]):
    def run(self, executable: BasicCallable, *args, **kwargs) -> int:
        with ExitStack() as stack:
            # IPC flow is the following:
            #  1. [controller]: Create the socket server
            #  2. [controller]: Spawn the isolated process with the socket address
            #  3.   [isolated]: Connect to the socket server
            #  4. [controller]: Accept the incoming connection request
            #  5. [controller]: Send the executable over the established bridge
            #  6.   [isolated]: Receive the executable from the bridge
            #  7.   [isolated]: Execute the executable and once done send the result back
            #  8. [controller]: Loop until either the isolated process exits or sends any
            #                   data (will be interpreted as a tuple of two mutually exclusive
            #                   objects, either a result object or an exception to be raised).
            #

            log_env(self.env, "Starting the controller bridge.")
            controller_service = stack.enter_context(bridge.controller_connection())

            log_env(
                self.env,
                "Controller server is listening at {}.",
                controller_service.address,
            )
            isolated_process = stack.enter_context(
                self.start_process(controller_service, *args, **kwargs)
            )

            log_env(
                self.env,
                "Awaiting child process of {} to establish a connection.",
                isolated_process.pid,
            )
            established_connection = stack.enter_context(controller_service.accept())

            log_env(
                self.env,
                "Bridge between controller and the child has been established at {}.",
                controller_service.address,
            )
            established_connection.send(executable)

            log_env(
                self.env,
                "Executable has been sent, awaiting execution result and logs.",
            )
            return self.poll_until_result(isolated_process, established_connection)

    def start_process(
        self, connection: bridge.ConnectionWrapper, *args, **kwargs
    ) -> ContextManager[subprocess.Popen]:
        raise NotImplementedError

    def poll_until_result(
        self, process: subprocess.Popen, connection: bridge.ConnectionWrapper
    ) -> Any:
        while process.poll() is None:
            # Normally, if we do connection.read() without having this loop
            # it is going to block us indefinitely (even if the underlying
            # process has crashed). We can use a combination of process.poll
            # and connection.poll to check if the process is alive and has data
            # to move forward.
            if not connection.poll():
                continue

            try:
                result, exception = connection.recv()
            except EOFError:
                raise RuntimeError("The isolated process has unexpectedly exited.")

            if exception is None:
                log_env(
                    self.env, "Isolated process has returned the result: {}", result
                )
                return result
            else:
                log_env(
                    self.env,
                    "The isolated process has exited with an exception.",
                    kind="error",
                )
                raise exception
