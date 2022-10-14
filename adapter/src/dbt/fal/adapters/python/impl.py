import abc
from contextlib import contextmanager
import time
from typing import (
    Optional,
    Type,
    Dict,
    Any,
    Mapping,
    Iterator,
    Union,
)
from dbt.adapters.factory import get_adapter


from dbt.exceptions import (
    NotImplementedException,
    RuntimeException,
)

from dbt.adapters.protocol import AdapterConfig, ConnectionManagerProtocol
from dbt.contracts.graph.compiled import CompileResultNode, CompiledSeedNode
from dbt.contracts.graph.parsed import ParsedSeedNode
from dbt.events.functions import fire_event
from dbt.events.types import (
    CodeExecution,
    CodeExecutionStatus,
)

from dbt.adapters.base.meta import AdapterMeta, available
from dbt.contracts.connection import Credentials, Connection, AdapterResponse


SeedModel = Union[ParsedSeedNode, CompiledSeedNode]


def log_code_execution(code_execution_function):
    # decorator to log code and execution time
    if code_execution_function.__name__ != "submit_python_job":
        raise ValueError("this should be only used to log submit_python_job now")

    def execution_with_log(*args):
        self = args[0]
        connection_name = self.connections.get_thread_connection().name
        fire_event(CodeExecution(conn_name=connection_name, code_content=args[2]))
        start_time = time.time()
        response = code_execution_function(*args)
        fire_event(
            CodeExecutionStatus(
                status=response._message, elapsed=round((time.time() - start_time), 2)
            )
        )
        return response

    return execution_with_log


class PythonJobHelper:
    def __init__(self, parsed_model: Dict, credential: Credentials) -> None:
        raise NotImplementedError("PythonJobHelper is not implemented yet")

    def submit(self, compiled_code: str) -> Any:
        raise NotImplementedError(
            "PythonJobHelper submit function is not implemented yet"
        )


class PythonAdapter(metaclass=AdapterMeta):
    """The PythonAdapter provides an abstract base class for Python adapters.

    Adapters must implement the following methods and macros. Some of the
    methods can be safely overridden as a noop, where it makes sense. Those
    methods are marked with a (passable) in their docstrings. Check docstrings
    for type information, etc.

    To implement a macro, implement "${adapter_type}__${macro_name}" in the
    adapter's internal project.

    To invoke a method in an adapter macro, call it on the 'adapter' Jinja
    object using dot syntax.

    To invoke a method in model code, add the @available decorator atop a method
    declaration. Methods are invoked as macros.

    Methods:
        - is_cancelable

        - python_submission_helpers
        - default_python_submission_method
        - generate_python_submission_response
        or
        - submit_python_job

    Macros:

    """

    ConnectionManager: Type[ConnectionManagerProtocol]

    # A set of clobber config fields accepted by this adapter
    # for use in materializations
    AdapterSpecificConfigs: Type[AdapterConfig] = AdapterConfig

    def __init__(self, config):
        self.config = config
        self.connections = self.ConnectionManager(config)

        # HACK: A Python adapter does not have _available_ all the attributes a DB adapter does.
        # Since we use the DB adapter as the storage for the Python adapter, we must proxy to it
        # all the unhandled calls.
        #
        # Another option is to write a PythonAdapter-specific DBWrapper (PythonWrapper?) that is
        # aware of this case. This may be appealing because a _complete_ adapter (DB+Python) would
        # then be more easily used to replace the Python part of any other adapter.

        self._db_adapter = get_adapter(config)
        self.Relation = self._db_adapter.Relation
        self.Column = self._db_adapter.Column

    def cache_added(self, *args, **kwargs):
        return self._db_adapter.cache_added(*args, **kwargs)

    @available.parse_none
    def get_relation(self, *args, **kwargs):
        return self._db_adapter.get_relation(*args, **kwargs)

    @classmethod
    def date_function(cls):
        # HACK: to appease the ProviderContext
        return """
        import datetime
        return datetime.datetime.now()
        """

    ###
    # Methods that pass through to the connection manager
    ###
    def acquire_connection(self, name=None) -> Connection:
        return self.connections.set_connection_name(name)

    def release_connection(self) -> None:
        self.connections.release()

    def cleanup_connections(self) -> None:
        self.connections.cleanup_all()

    def nice_connection_name(self) -> str:
        conn = self.connections.get_if_exists()
        if conn is None or conn.name is None:
            return "<None>"
        return conn.name

    @contextmanager
    def connection_named(
        self, name: str, node: Optional[CompileResultNode] = None
    ) -> Iterator[None]:
        try:
            self.acquire_connection(name)
            yield
        finally:
            self.release_connection()

    @contextmanager
    def connection_for(self, node: CompileResultNode) -> Iterator[None]:
        with self.connection_named(node.unique_id, node):
            yield

    @classmethod
    @abc.abstractmethod
    def is_cancelable(cls) -> bool:
        raise NotImplementedException(
            "`is_cancelable` is not implemented for this adapter!"
        )

    ###
    # Methods that should never be overridden
    ###
    @classmethod
    def type(cls) -> str:
        """Get the type of this adapter. Types must be class-unique and
        consistent.

        :return: The type name
        :rtype: str
        """
        return cls.ConnectionManager.TYPE

    ###
    # ODBC FUNCTIONS -- these should not need to change for every adapter,
    #                   although some adapters may override them
    ###
    def cancel_open_connections(self):
        """Cancel all open connections."""
        return self.connections.cancel_open()

    def pre_model_hook(self, config: Mapping[str, Any]) -> Any:
        """A hook for running some operation before the model materialization
        runs. The hook can assume it has a connection available.

        The only parameter is a configuration dictionary (the same one
        available in the materialization context). It should be considered
        read-only.

        The pre-model hook may return anything as a context, which will be
        passed to the post-model hook.
        """
        pass

    def post_model_hook(self, config: Mapping[str, Any], context: Any) -> None:
        """A hook for running some operation after the model materialization
        runs. The hook can assume it has a connection available.

        The first parameter is a configuration dictionary (the same one
        available in the materialization context). It should be considered
        read-only.

        The second parameter is the value returned by pre_mdoel_hook.
        """
        pass

    def get_compiler(self):
        from dbt.compilation import Compiler

        return Compiler(self.config)

    ###
    # Python
    ###
    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        raise NotImplementedError("python_submission_helpers is not specified")

    @property
    def default_python_submission_method(self) -> str:
        raise NotImplementedError("default_python_submission_method is not specified")

    @log_code_execution
    def submit_python_job(
        self, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        submission_method = parsed_model["config"].get(
            "submission_method", self.default_python_submission_method
        )
        if submission_method not in self.python_submission_helpers:
            raise NotImplementedError(
                "Submission method {} is not supported for current adapter".format(
                    submission_method
                )
            )
        job_helper = self.python_submission_helpers[submission_method](
            parsed_model, self.connections.credentials
        )
        submission_result = job_helper.submit(compiled_code)
        # process submission result to generate adapter response
        return self.generate_python_submission_response(submission_result)

    def generate_python_submission_response(
        self, submission_result: Any
    ) -> AdapterResponse:
        raise NotImplementedException(
            "Your adapter need to implement generate_python_submission_response"
        )

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append"]

    def builtin_incremental_strategies(self):
        return ["append", "delete+insert", "merge", "insert_overwrite"]

    @available.parse_none
    def get_incremental_strategy_macro(self, model_context, strategy: str):
        # Construct macro_name from strategy name
        if strategy is None:
            strategy = "default"

        # validate strategies for this adapter
        valid_strategies = self.valid_incremental_strategies()
        valid_strategies.append("default")
        builtin_strategies = self.builtin_incremental_strategies()
        if strategy in builtin_strategies and strategy not in valid_strategies:
            raise RuntimeException(
                f"The incremental strategy '{strategy}' is not valid for this adapter"
            )

        strategy = strategy.replace("+", "_")
        macro_name = f"get_incremental_{strategy}_sql"
        # The model_context should have MacroGenerator callable objects for all macros
        if macro_name not in model_context:
            raise RuntimeException(
                'dbt could not find an incremental strategy macro with the name "{}" in {}'.format(
                    macro_name, self.config.project_name
                )
            )

        # This returns a callable macro
        return model_context[macro_name]
