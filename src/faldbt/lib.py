# NOTE: COPIED FROM https://github.com/dbt-labs/dbt-core/blob/40ae6b6bc860a30fa383756b7cdb63709ce829a8/core/dbt/lib.py
import six

from datetime import datetime
from uuid import uuid4
from typing import List, Tuple, Union

from dbt.config.runtime import RuntimeConfig
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import Manifest
import dbt.adapters.factory as adapters_factory
from dbt.logger import GLOBAL_LOGGER as logger

from . import parse

import pandas as pd
from pandas.io import sql as pdsql

import sqlalchemy
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql import Insert
from sqlalchemy.sql.schema import MetaData

from faldbt.cp.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
from faldbt.cp.contracts.sql import ResultTable, RemoteRunResult


def register_adapters(config: RuntimeConfig):
    # Clear previously registered adapters. This fixes cacheing behavior on the dbt-server
    adapters_factory.reset_adapters()
    # Load the relevant adapter
    adapters_factory.register_adapter(config)


def _get_operation_node(manifest: Manifest, project_path, profiles_dir, sql):
    from dbt.parser.manifest import process_node
    from faldbt.cp.parser.sql import SqlBlockParser

    config = parse.get_dbt_config(project_path, profiles_dir)
    block_parser = SqlBlockParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )

    # NOTE: nodes get registered to the manifest automatically,
    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "SQL:" + str(hash(sql)) + ":" + str(uuid4())
    sql_node = block_parser.parse_remote(sql, name)
    process_node(config, manifest, sql_node)
    return sql_node


# NOTE: Once we get an adapter, we must call `connection_for` or `connection_named` to use it
def _get_adapter(project_path: str, profiles_dir: str):
    config = parse.get_dbt_config(project_path, profiles_dir)

    adapters_factory.cleanup_connections()
    return adapters_factory.get_adapter(config)


def _execute_sql(
    manifest: Manifest, project_path: str, profiles_dir: str, sql: str
) -> Tuple[AdapterResponse, RemoteRunResult]:
    node = _get_operation_node(manifest, project_path, profiles_dir, sql)
    adapter = _get_adapter(project_path, profiles_dir)

    logger.info("Running query\n{}", sql)

    result = None
    with adapter.connection_for(node):
        adapter.connections.begin()
        response, execute_result = adapter.execute(sql, fetch=True)

        table = ResultTable(
            column_names=list(execute_result.column_names),
            rows=[list(row) for row in execute_result],
        )

        result = RemoteRunResult(
            raw_sql=sql,
            compiled_sql=sql,
            node=node,
            table=table,
            timing=[],
            logs=[],
            generated_at=datetime.utcnow(),
        )
        adapter.connections.commit()

    return response, result


def _get_target_relation(
    target: Union[ParsedModelNode, ParsedSourceDefinition],
    project_path: str,
    profiles_dir: str,
):
    adapter = _get_adapter(project_path, profiles_dir)

    relation = None
    with adapter.connection_named(str(uuid4())):
        # This ROLLBACKs so it has to be a new connection
        relation = adapter.get_relation(
            target.database, target.schema, target.identifier
        )
    return relation


def execute_sql(
    manifest: Manifest, project_path: str, profiles_dir: str, sql: str
) -> RemoteRunResult:
    _, result = _execute_sql(manifest, project_path, profiles_dir, sql)
    return result


def fetch_target(
    manifest: Manifest,
    project_path: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
) -> RemoteRunResult:
    relation = _get_target_relation(target, project_path, profiles_dir)

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    query = f"SELECT * FROM {relation}"
    _, result = _execute_sql(manifest, project_path, profiles_dir, query)
    return result


def write_target(
    data: pd.DataFrame,
    manifest: Manifest,
    project_path: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
) -> RemoteRunResult:
    adapter = _get_adapter(project_path, profiles_dir)

    relation = _get_target_relation(target, project_path, profiles_dir)

    engine = _alchemy_engine(adapter, target)
    pddb = pdsql.SQLDatabase(
        engine,
        meta=MetaData(engine, schema=target.schema),
    )
    pdtable = pdsql.SQLTable(target.name, pddb, data, index=False)
    alchemy_table: sqlalchemy.Table = pdtable.table.to_metadata(pdtable.pd_sql.meta)

    column_names: List[str] = list(data.columns)
    rows = data.to_records(index=False)
    row_dicts = list(map(lambda row: dict(zip(column_names, row)), rows))

    if relation is None:
        create_stmt = CreateTable(alchemy_table).compile(
            bind=engine, compile_kwargs={"literal_binds": True}
        )

        _execute_sql(
            manifest, project_path, profiles_dir, six.text_type(create_stmt).strip()
        )

    insert_stmt = (
        Insert(alchemy_table)
        .values(row_dicts)
        .compile(bind=engine, compile_kwargs={"literal_binds": True})
    )

    _, result = _execute_sql(
        manifest, project_path, profiles_dir, six.text_type(insert_stmt).strip()
    )
    return result


def _alchemy_engine(
    adapter: adapters_factory.Adapter,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
):
    if adapter.type() == "bigquery":
        return sqlalchemy.create_engine(f"bigquery://{target.database}")
    if adapter.type() == "postgres":
        return sqlalchemy.create_engine("postgresql://")
    else:
        # TODO: add special cases as needed
        return sqlalchemy.create_engine(f"{adapter.type()}://")
