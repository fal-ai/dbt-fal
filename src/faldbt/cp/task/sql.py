# NOTE: COPIED FROM https://github.com/dbt-labs/dbt-core/blob/89907f09c8a94d95d2882f909f05143e098746a0/core/dbt/task/sql.py
from abc import abstractmethod
from datetime import datetime
from typing import Generic, TypeVar

import dbt.exceptions
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.compile import CompileRunner

from faldbt.cp.contracts.sql import (
    RemoteCompileResult,
    RemoteCompileResultMixin,
    RemoteRunResult,
    ResultTable,
)


SQLResult = TypeVar('SQLResult', bound=RemoteCompileResultMixin)


class GenericSqlRunner(CompileRunner, Generic[SQLResult]):
    def __init__(self, config, adapter, node, node_index, num_nodes):
        CompileRunner.__init__(
            self, config, adapter, node, node_index, num_nodes
        )

    def handle_exception(self, e, ctx):
        logger.debug('Got an exception: {}'.format(e), exc_info=True)
        if isinstance(e, dbt.exceptions.Exception):
            if isinstance(e, dbt.exceptions.RuntimeException):
                e.add_node(ctx.node)
            return e

    def before_execute(self):
        pass

    def after_execute(self, result):
        pass

    def compile(self, manifest):
        compiler = self.adapter.get_compiler()
        return compiler.compile_node(self.node, manifest, {}, write=False)

    @abstractmethod
    def execute(self, compiled_node, manifest) -> SQLResult:
        pass

    @abstractmethod
    def from_run_result(self, result, start_time, timing_info) -> SQLResult:
        pass

    def error_result(self, node, error, start_time, timing_info):
        raise error

    def ephemeral_result(self, node, start_time, timing_info):
        raise dbt.exceptions.NotImplementedException(
            'cannot execute ephemeral nodes remotely!'
        )


class SqlCompileRunner(GenericSqlRunner[RemoteCompileResult]):
    def execute(self, compiled_node, manifest) -> RemoteCompileResult:
        return RemoteCompileResult(
            raw_sql=compiled_node.raw_sql,
            compiled_sql=compiled_node.compiled_sql,
            node=compiled_node,
            timing=[],  # this will get added later
            logs=[],
            generated_at=datetime.utcnow(),
        )

    def from_run_result(
        self, result, start_time, timing_info
    ) -> RemoteCompileResult:
        return RemoteCompileResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            timing=timing_info,
            logs=[],
            generated_at=datetime.utcnow(),
        )


class SqlExecuteRunner(GenericSqlRunner[RemoteRunResult]):
    def execute(self, compiled_node, manifest) -> RemoteRunResult:
        _, execute_result = self.adapter.execute(
            compiled_node.compiled_sql, fetch=True
        )

        table = ResultTable(
            column_names=list(execute_result.column_names),
            rows=[list(row) for row in execute_result],
        )

        return RemoteRunResult(
            raw_sql=compiled_node.raw_sql,
            compiled_sql=compiled_node.compiled_sql,
            node=compiled_node,
            table=table,
            timing=[],
            logs=[],
            generated_at=datetime.utcnow(),
        )

    def from_run_result(
        self, result, start_time, timing_info
    ) -> RemoteRunResult:
        return RemoteRunResult(
            raw_sql=result.raw_sql,
            compiled_sql=result.compiled_sql,
            node=result.node,
            table=result.table,
            timing=timing_info,
            logs=[],
            generated_at=datetime.utcnow(),
        )
