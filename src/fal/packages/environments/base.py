from __future__ import annotations

import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Callable, ContextManager, Iterator

from dbt.logger import GLOBAL_LOGGER as logger
from platformdirs import user_cache_dir

from fal.packages import bridge

BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
BASE_CACHE_DIR.mkdir(exist_ok=True)

InternalHook = Callable[[], int]

if TYPE_CHECKING:
    from typing import Protocol

    class HookRunnerProtocol(Protocol):
        def __call__(self, executable: InternalHook, *args, **kwargs) -> int:
            ...


@contextmanager
def rmdir_on_fail(path: Path) -> Iterator[None]:
    try:
        yield
    except Exception:
        shutil.rmtree(path)
        raise


class BaseEnvironment:
    @property
    def key(self) -> str:
        """A unique identifier for this environment (combination of requirements,
        python version and other information) that can be used for caching."""
        raise NotImplementedError

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
