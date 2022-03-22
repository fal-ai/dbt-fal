# NOTE: INSPIRED IN https://github.com/dbt-labs/dbt-core/blob/43edc887f97e359b02b6317a9f91898d3d66652b/core/dbt/lib.py
import six
from datetime import datetime
from dataclasses import dataclass
from uuid import uuid4
from typing import List, Tuple, Union

import dbt.version
import dbt.semver
import dbt.flags as flags
import dbt.adapters.factory as adapters_factory
from dbt.contracts.connection import AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.sql import SQLAdapter

from . import parse

import pandas as pd
from pandas.io import sql as pdsql

import sqlalchemy
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql import Insert

DBT_V1 = dbt.semver.VersionSpecifier.from_version_string("1.0.0")
DBT_VCURRENT = dbt.version.get_installed_version()

if DBT_VCURRENT.compare(DBT_V1) >= 0:
    from dbt.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
    from dbt.contracts.sql import ResultTable, RemoteRunResult
else:
    from faldbt.cp.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
    from faldbt.cp.contracts.sql import ResultTable, RemoteRunResult


@dataclass
class FlagsArgs:
    profiles_dir: str
    use_colors: bool


def initialize_dbt_flags(profiles_dir: str):
    """
    Initializes the flags module from dbt, since it's accessed from around their code.
    """
    args = FlagsArgs(profiles_dir, None)
    user_config = parse.get_dbt_user_config(profiles_dir)
    try:
        flags.set_from_args(args, user_config)
    except TypeError:
        flags.set_from_args(args)

    # Set invocation id
    if DBT_VCURRENT.compare(DBT_V1) >= 0:
        import dbt.events.functions as events_functions

        events_functions.set_invocation_id()

    # Re-enable logging for 1.0.0 through old API of logger
    # TODO: migrate for 1.0.0 code to new event system
    if DBT_VCURRENT.compare(DBT_V1) >= 0:
        flags.ENABLE_LEGACY_LOGGER = "1"


# NOTE: Once we get an adapter, we must call `connection_for` or `connection_named` to use it
def _get_adapter(project_path: str, profiles_dir: str) -> SQLAdapter:
    config = parse.get_dbt_config(project_path, profiles_dir)

    return adapters_factory.get_adapter(config)  # type: ignore


def _execute_sql(
    project_path: str, profiles_dir: str, sql: str
) -> Tuple[AdapterResponse, RemoteRunResult]:
    adapter = _get_adapter(project_path, profiles_dir)

    logger.debug("Running query\n{}", sql)

    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "SQL:" + str(hash(sql)) + ":" + str(uuid4())
    result = None
    with adapter.connection_named(name):
        response, execute_result = adapter.execute(sql, auto_begin=True, fetch=True)

        table = ResultTable(
            column_names=list(execute_result.column_names),
            rows=[list(row) for row in execute_result],
        )

        result = RemoteRunResult(
            raw_sql=sql,
            compiled_sql=sql,
            node=None,
            table=table,
            timing=[],
            logs=[],
            generated_at=datetime.utcnow(),
        )
        adapter.commit_if_has_connection()

    return response, result


def _get_target_relation(
    target: Union[ParsedModelNode, ParsedSourceDefinition],
    project_path: str,
    profiles_dir: str,
):
    adapter = _get_adapter(project_path, profiles_dir)

    name = "relation:" + str(hash(str(target))) + ":" + str(uuid4())
    relation = None
    with adapter.connection_named(name):
        # This ROLLBACKs so it has to be a new connection
        relation = adapter.get_relation(
            target.database, target.schema, target.identifier
        )
    return relation


def execute_sql(project_path: str, profiles_dir: str, sql: str) -> RemoteRunResult:
    _, result = _execute_sql(project_path, profiles_dir, sql)
    return result


def fetch_target(
    project_path: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
) -> RemoteRunResult:
    relation = _get_target_relation(target, project_path, profiles_dir)

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    query = f"SELECT * FROM {relation}"
    _, result = _execute_sql(project_path, profiles_dir, query)
    return result


def write_target(
    data: pd.DataFrame,
    project_path: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
    dtype=None,
) -> RemoteRunResult:
    adapter = _get_adapter(project_path, profiles_dir)

    relation = _get_target_relation(target, project_path, profiles_dir)

    engine = _alchemy_engine(adapter, target)
    pddb = pdsql.SQLDatabase(engine, schema=target.schema)
    pdtable = pdsql.SQLTable(target.name, pddb, data, index=False, dtype=dtype)
    alchemy_table: sqlalchemy.Table = pdtable.table.to_metadata(pdtable.pd_sql.meta)

    column_names: List[str] = list(data.columns)

    rows = data.to_records(index=False)
    row_dicts = list(map(lambda row: dict(zip(column_names, row)), rows))

    if relation is None:
        create_stmt = CreateTable(alchemy_table).compile(
            bind=engine, compile_kwargs={"literal_binds": True}
        )

        _execute_sql(project_path, profiles_dir, six.text_type(create_stmt).strip())

    insert_stmt = (
        Insert(alchemy_table)
        .values(row_dicts)
        .compile(bind=engine, compile_kwargs={"literal_binds": True})
    )

    _, result = _execute_sql(
        project_path, profiles_dir, six.text_type(insert_stmt).strip()
    )
    return result


def _alchemy_engine(
    adapter: adapters_factory.Adapter,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
):
    url_string = f"{adapter.type()}://"
    if adapter.type() == "bigquery":
        url_string = f"bigquery://{target.database}"
    if adapter.type() == "postgres":
        url_string = "postgresql://"
    # TODO: add special cases as needed

    def null_dump(sql, *multiparams, **params):
        pass

    return sqlalchemy.create_mock_engine(url_string, executor=null_dump)
