import abc
import os
from time import sleep
import sys

# multiprocessing.RLock is a function returning this type
from multiprocessing.synchronize import RLock
from threading import get_ident
from typing import (
    Any,
    Dict,
    Tuple,
    Hashable,
    Optional,
    List,
    Type,
    Union,
    Iterable,
    Callable,
)

import dbt.exceptions
from dbt.contracts.connection import (
    Connection,
    Identifier,
    ConnectionState,
    AdapterRequiredConfig,
    LazyHandle,
    AdapterResponse,
)
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import (
    NewConnection,
    ConnectionReused,
    ConnectionLeftOpen,
    ConnectionLeftOpen2,
    ConnectionClosed,
    ConnectionClosed2,
)
from dbt import flags

SleepTime = Union[int, float]  # As taken by time.sleep.
AdapterHandle = Any  # Adapter connection handle objects can be any class.


class PythonConnectionManager(metaclass=abc.ABCMeta):
    """Methods to implement:
        - open
        - execute

    You must also set the 'TYPE' class attribute with a class-unique constant
    string.
    """

    TYPE: str = NotImplemented

    def __init__(self, profile: AdapterRequiredConfig):
        self.profile = profile
        if profile.credentials.type == self.TYPE:
            self.credentials = profile.credentials
        else:
            self.credentials = profile.python_adapter_credentials

        self.thread_connections: Dict[Hashable, Connection] = {}
        self.lock: RLock = flags.MP_CONTEXT.RLock()

    @staticmethod
    def get_thread_identifier() -> Hashable:
        # note that get_ident() may be re-used, but we should never experience
        # that within a single process
        return (os.getpid(), get_ident())

    def get_thread_connection(self) -> Connection:
        key = self.get_thread_identifier()
        with self.lock:
            if key not in self.thread_connections:
                raise dbt.exceptions.InvalidConnectionException(
                    key, list(self.thread_connections)
                )
            return self.thread_connections[key]

    def set_thread_connection(self, conn: Connection) -> None:
        key = self.get_thread_identifier()
        if key in self.thread_connections:
            raise dbt.exceptions.InternalException(
                "In set_thread_connection, existing connection exists for {}"
            )
        self.thread_connections[key] = conn

    def get_if_exists(self) -> Optional[Connection]:
        key = self.get_thread_identifier()
        with self.lock:
            return self.thread_connections.get(key)

    def clear_thread_connection(self) -> None:
        key = self.get_thread_identifier()
        with self.lock:
            if key in self.thread_connections:
                del self.thread_connections[key]

    def set_connection_name(self, name: Optional[str] = None) -> Connection:
        conn_name: str
        if name is None:
            # if a name isn't specified, we'll re-use a single handle
            # named 'master'
            conn_name = "master"
        else:
            if not isinstance(name, str):
                raise dbt.exceptions.CompilerException(
                    f"For connection name, got {name} - not a string!"
                )
            assert isinstance(name, str)
            conn_name = name

        conn = self.get_if_exists()
        if conn is None:
            conn = Connection(
                type=Identifier(self.TYPE),
                name=None,
                state=ConnectionState.INIT,
                transaction_open=False,
                handle=None,
                credentials=self.credentials,
            )
            self.set_thread_connection(conn)

        if conn.name == conn_name and conn.state == "open":
            return conn

        fire_event(NewConnection(conn_name=conn_name, conn_type=self.TYPE))

        if conn.state == "open":
            fire_event(ConnectionReused(conn_name=conn_name))
        else:
            conn.handle = LazyHandle(self.open)

        conn.name = conn_name
        return conn

    @classmethod
    def retry_connection(
        cls,
        connection: Connection,
        connect: Callable[[], AdapterHandle],
        logger: AdapterLogger,
        retryable_exceptions: Iterable[Type[Exception]],
        retry_limit: int = 1,
        retry_timeout: Union[Callable[[int], SleepTime], SleepTime] = 1,
        _attempts: int = 0,
    ) -> Connection:
        """Given a Connection, set its handle by calling connect.

        The calls to connect will be retried up to retry_limit times to deal with transient
        connection errors. By default, one retry will be attempted if retryable_exceptions is set.

        :param Connection connection: An instance of a Connection that needs a handle to be set,
            usually when attempting to open it.
        :param connect: A callable that returns the appropiate connection handle for a
            given adapter. This callable will be retried retry_limit times if a subclass of any
            Exception in retryable_exceptions is raised by connect.
        :type connect: Callable[[], AdapterHandle]
        :param AdapterLogger logger: A logger to emit messages on retry attempts or errors. When
            handling expected errors, we call debug, and call warning on unexpected errors or when
            all retry attempts have been exhausted.
        :param retryable_exceptions: An iterable of exception classes that if raised by
            connect should trigger a retry.
        :type retryable_exceptions: Iterable[Type[Exception]]
        :param int retry_limit: How many times to retry the call to connect. If this limit
            is exceeded before a successful call, a FailedToConnectException will be raised.
            Must be non-negative.
        :param retry_timeout: Time to wait between attempts to connect. Can also take a
            Callable that takes the number of attempts so far, beginning at 0, and returns an int
            or float to be passed to time.sleep.
        :type retry_timeout: Union[Callable[[int], SleepTime], SleepTime] = 1
        :param int _attempts: Parameter used to keep track of the number of attempts in calling the
            connect function across recursive calls. Passed as an argument to retry_timeout if it
            is a Callable. This parameter should not be set by the initial caller.
        :raises dbt.exceptions.FailedToConnectException: Upon exhausting all retry attempts without
            successfully acquiring a handle.
        :return: The given connection with its appropriate state and handle attributes set
            depending on whether we successfully acquired a handle or not.
        """
        timeout = retry_timeout(_attempts) if callable(retry_timeout) else retry_timeout
        if timeout < 0:
            raise dbt.exceptions.FailedToConnectException(
                "retry_timeout cannot be negative or return a negative time."
            )

        if retry_limit < 0 or retry_limit > sys.getrecursionlimit():
            # This guard is not perfect others may add to the recursion limit (e.g. built-ins).
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise dbt.exceptions.FailedToConnectException(
                "retry_limit cannot be negative"
            )

        try:
            connection.handle = connect()
            connection.state = ConnectionState.OPEN
            return connection

        except tuple(retryable_exceptions) as e:
            if retry_limit <= 0:
                connection.handle = None
                connection.state = ConnectionState.FAIL
                raise dbt.exceptions.FailedToConnectException(str(e))

            logger.debug(
                f"Got a retryable error when attempting to open a {cls.TYPE} connection.\n"
                f"{retry_limit} attempts remaining. Retrying in {timeout} seconds.\n"
                f"Error:\n{e}"
            )

            sleep(timeout)
            return cls.retry_connection(
                connection=connection,
                connect=connect,
                logger=logger,
                retry_limit=retry_limit - 1,
                retry_timeout=retry_timeout,
                retryable_exceptions=retryable_exceptions,
                _attempts=_attempts + 1,
            )

        except Exception as e:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise dbt.exceptions.FailedToConnectException(str(e))

    @abc.abstractmethod
    def cancel(self, connection: Connection):
        """Cancel the given connection."""
        raise dbt.exceptions.NotImplementedException(
            "`cancel` is not implemented for this adapter!"
        )

    def cancel_open(self) -> List[str]:
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for connection in self.thread_connections.values():
                if connection is this_connection:
                    continue

                # if the connection failed, the handle will be None so we have
                # nothing to cancel.
                if (
                    connection.handle is not None
                    and connection.state == ConnectionState.OPEN
                ):
                    self.cancel(connection)
                if connection.name is not None:
                    names.append(connection.name)
        return names

    @classmethod
    @abc.abstractmethod
    def open(cls, connection: Connection) -> Connection:
        """Open the given connection on the adapter and return it.

        This may mutate the given connection (in particular, its state and its
        handle).

        This should be thread-safe, or hold the lock if necessary. The given
        connection should not be in either in_use or available.
        """
        raise dbt.exceptions.NotImplementedException(
            "`open` is not implemented for this adapter!"
        )

    def release(self) -> None:
        with self.lock:
            conn = self.get_if_exists()
            if conn is None:
                return

        try:
            # always close the connection
            self.close(conn)
        except Exception:
            # if rollback or close failed, remove our busted connection
            self.clear_thread_connection()
            raise

    def cleanup_all(self) -> None:
        with self.lock:
            for connection in self.thread_connections.values():
                if connection.state not in {"closed", "init"}:
                    fire_event(ConnectionLeftOpen(conn_name=connection.name))
                else:
                    fire_event(ConnectionClosed(conn_name=connection.name))
                self.close(connection)

            # garbage collect these connections
            self.thread_connections.clear()

    @classmethod
    def _close_handle(cls, connection: Connection) -> None:
        """Perform the actual close operation."""
        # On windows, sometimes connection handles don't have a close() attr.
        if hasattr(connection.handle, "close"):
            fire_event(ConnectionClosed2(conn_name=connection.name))
            connection.handle.close()
        else:
            fire_event(ConnectionLeftOpen2(conn_name=connection.name))

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        # if the connection is in closed or init, there's nothing to do
        if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
            return connection

        cls._close_handle(connection)
        connection.state = ConnectionState.CLOSED

        return connection

    @abc.abstractmethod
    def execute(self, compiled_code: str) -> Tuple[AdapterResponse, Any]:
        """Execute the given Python code.

        :param str compiled_code: The Python code to execute.
        :return: A tuple of the run status and results
                (type is left to be decided by the Adapter implementation for now).
        :rtype: Tuple[AdapterResponse, Any]
        """
        raise dbt.exceptions.NotImplementedException(
            "`execute` is not implemented for this adapter!"
        )
