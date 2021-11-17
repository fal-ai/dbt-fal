# NOTE: COPIED FROM https://github.com/dbt-labs/dbt-core/blob/40ae6b6bc860a30fa383756b7cdb63709ce829a8/core/dbt/lib.py
import os
import six

from datetime import datetime
from uuid import uuid4
from collections import namedtuple
from typing import List, Tuple, Type, Union

from dbt.config.runtime import RuntimeConfig
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.graph.manifest import Manifest
import dbt.clients.agate_helper as agate_helper
import dbt.adapters.factory as adapters_factory

import pandas as pd

from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql import Insert

from faldbt.cp.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
from faldbt.cp.contracts.sql import ResultTable, RemoteRunResult

import agatesql
import agatesql.table

# from sqlalchemy.engine import Connection as SQLAlchemyConnection

RuntimeArgs = namedtuple("RuntimeArgs", "project_dir profiles_dir single_threaded")


def get_dbt_config(project_dir: str, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig

    if os.getenv("DBT_PROFILES_DIR"):
        profiles_dir = os.getenv("DBT_PROFILES_DIR")
    else:
        profiles_dir = os.path.expanduser("~/.dbt")

    # Construct a phony config
    return RuntimeConfig.from_args(
        RuntimeArgs(project_dir, profiles_dir, single_threaded)
    )


def register_adapters(config: RuntimeConfig):
    # Clear previously registered adapters. This fixes cacheing behavior on the dbt-server
    adapters_factory.reset_adapters()
    # Load the relevant adapter
    adapters_factory.register_adapter(config)


def _get_operation_node(manifest: Manifest, project_path, sql):
    from dbt.parser.manifest import process_node
    from faldbt.cp.parser.sql import SqlBlockParser

    config = get_dbt_config(project_path)
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
def _get_adapter(project_path: str):
    config = get_dbt_config(project_path)

    adapters_factory.cleanup_connections()
    return adapters_factory.get_adapter(config)


def _execute_sql(
    manifest: Manifest, project_path: str, sql: str
) -> Tuple[AdapterResponse, RemoteRunResult]:
    node = _get_operation_node(manifest, project_path, sql)
    adapter = _get_adapter(project_path)

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
    target: Union[ParsedModelNode, ParsedSourceDefinition], project_path: str
):
    adapter = _get_adapter(project_path)

    relation = None
    with adapter.connection_named(str(uuid4())):
        # This ROLLBACKs so it has to be a new connection
        relation = adapter.get_relation(
            target.database, target.schema, target.identifier
        )
    return relation


def execute_sql(manifest: Manifest, project_path: str, sql: str) -> RemoteRunResult:
    _, result = _execute_sql(manifest, project_path, sql)
    return result


def fetch_target(
    manifest: Manifest,
    project_path: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
) -> RemoteRunResult:
    relation = _get_target_relation(target, project_path)

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    query = f"SELECT * FROM {relation}"
    _, result = _execute_sql(manifest, project_path, query)
    return result


def write_target(
    data: pd.DataFrame,
    manifest: Manifest,
    project_path: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
) -> RemoteRunResult:
    relation = _get_target_relation(target, project_path)

    column_names: List[str] = list(data.columns)
    rows = data.to_records(index=False)
    row_dicts = list(map(lambda row: dict(zip(column_names, row)), rows))

    agate_table = agate_helper.table_from_data(row_dicts, column_names=column_names)

    # We are using the SQLAlchemy table to generate the SQL, but it targeting sqlite
    # This may bite us later for other adapters
    alchemy_table = agatesql.table.make_sql_table(
        agate_table, table_name=target.identifier, db_schema=target.schema
    )

    if relation is None:
        create_stmt = CreateTable(alchemy_table).compile(
            compile_kwargs={"literal_binds": True}
        )

        _execute_sql(manifest, project_path, six.text_type(create_stmt).strip())

    insert_stmt = (
        Insert(alchemy_table)
        .values(row_dicts)
        .compile(compile_kwargs={"literal_binds": True})
    )

    _, result = _execute_sql(manifest, project_path, six.text_type(insert_stmt).strip())
    return result


def parse_to_manifest(config):
    from dbt.parser.manifest import ManifestLoader

    return ManifestLoader.get_full_manifest(config)
