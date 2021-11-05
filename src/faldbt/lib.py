# NOTE: COPIED FROM https://github.com/dbt-labs/dbt-core/blob/40ae6b6bc860a30fa383756b7cdb63709ce829a8/core/dbt/lib.py
from dbt.adapters.factory import Adapter
from dbt.contracts.graph.manifest import Manifest
import os
from collections import namedtuple
from faldbt.cp.contracts.graph.parsed import ParsedModelNode
from faldbt.cp.contracts.sql import ResultTable, RemoteRunResult
from datetime import datetime
from uuid import uuid4


RuntimeArgs = namedtuple(
    'RuntimeArgs', 'project_dir profiles_dir single_threaded'
)


def get_dbt_config(project_dir, single_threaded=False):
    from dbt.config.runtime import RuntimeConfig
    import dbt.adapters.factory

    if os.getenv('DBT_PROFILES_DIR'):
        profiles_dir = os.getenv('DBT_PROFILES_DIR')
    else:
        profiles_dir = os.path.expanduser("~/.dbt")

    # Construct a phony config
    config = RuntimeConfig.from_args(RuntimeArgs(
        project_dir, profiles_dir, single_threaded
    ))
    # Load the relevant adapter
    dbt.adapters.factory.register_adapter(config)

    return config

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
    name = 'SQL:' + str(hash(sql)) + ':' + str(uuid4())
    sql_node = block_parser.parse_remote(sql, name)
    process_node(config, manifest, sql_node)
    return sql_node

def _exec_execute(adapter, sql) -> RemoteRunResult:
    _, execute_result = adapter.execute(
        sql, fetch=True
    )

    table = ResultTable(
        column_names=list(execute_result.column_names),
        rows=[list(row) for row in execute_result],
    )

    return RemoteRunResult(
        raw_sql=sql,
        compiled_sql=sql,
        node=None,
        table=table,
        timing=[],
        logs=[],
        generated_at=datetime.utcnow(),
    )

def _get_adapter(project_path):
    config = get_dbt_config(project_path)

    import dbt.adapters.factory
    return dbt.adapters.factory.get_adapter(config)

def _execute_sql(manifest: Manifest, project_path: str, adapter: Adapter, sql: str):
    node = _get_operation_node(manifest, project_path, sql)

    result = None
    with adapter.connection_for(node):
        print(f'Running query:\n{sql}')
        result = _exec_execute(adapter, sql)

    return result


def execute_sql(manifest: Manifest, project_path: str, sql: str):
    adapter = _get_adapter(project_path)
    return _execute_sql(manifest, project_path, adapter, sql)


def fetch_model(manifest: Manifest, project_path: str, model: ParsedModelNode):
    adapter = _get_adapter(project_path)
    relation = adapter.get_relation(model.database, model.schema, model.identifier)
    query = f'SELECT * FROM {relation}'
    return _execute_sql(manifest, project_path, adapter, query)


def parse_to_manifest(config):
    from dbt.parser.manifest import ManifestLoader
    return ManifestLoader.get_full_manifest(config)
