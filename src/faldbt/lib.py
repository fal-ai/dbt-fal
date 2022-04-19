# NOTE: INSPIRED IN https://github.com/dbt-labs/dbt-core/blob/43edc887f97e359b02b6317a9f91898d3d66652b/core/dbt/lib.py
import six
from datetime import datetime
from dataclasses import dataclass
from uuid import uuid4
from typing import List, Optional, Tuple, Union

import dbt.version
import dbt.semver
import dbt.flags as flags
import dbt.adapters.factory as adapters_factory
from dbt.contracts.connection import AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base import BaseRelation

from . import parse

import pandas as pd
from pandas.io import sql as pdsql

import sqlalchemy
from sqlalchemy.sql.ddl import CreateTable, DropTable
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
def _get_adapter(project_dir: str, profiles_dir: str) -> SQLAdapter:
    config = parse.get_dbt_config(project_dir, profiles_dir)

    adapter: SQLAdapter = adapters_factory.get_adapter(config)  # type: ignore

    return adapter


def _execute_sql(
    project_dir: str, profiles_dir: str, sql: str
) -> Tuple[AdapterResponse, RemoteRunResult]:
    adapter = _get_adapter(project_dir, profiles_dir)

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
    project_dir: str,
    profiles_dir: str,
    default: bool = False,
) -> Optional[BaseRelation]:
    adapter = _get_adapter(project_dir, profiles_dir)

    name = "relation:" + str(hash(str(target))) + ":" + str(uuid4())
    relation = None
    with adapter.connection_named(name):
        # This ROLLBACKs so it has to be a new connection
        relation = adapter.get_relation(target.database, target.schema, target.name)

    if relation is None and default:
        from dbt.contracts.relation import Path, RelationType

        path = Path(target.database, target.schema, target.identifier)
        # NOTE: assuming we want TABLE relation if not found
        relation = BaseRelation(path, type=RelationType.Table)

    return relation


def execute_sql(project_dir: str, profiles_dir: str, sql: str) -> RemoteRunResult:
    _, result = _execute_sql(project_dir, profiles_dir, sql)
    return result


def fetch_target(
    project_dir: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
) -> RemoteRunResult:
    relation = _get_target_relation(target, project_dir, profiles_dir)

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    query = f"SELECT * FROM {relation}"
    _, result = _execute_sql(project_dir, profiles_dir, query)
    return result


def overwrite_target(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
    dtype=None,
) -> RemoteRunResult:
    relation: BaseRelation = _get_target_relation(  # type: ignore
        target, project_dir, profiles_dir, default=True
    )
    from dbt.contracts.relation import Path, RelationType

    temporal_relation = BaseRelation(
        Path(relation.database, relation.schema, f"{relation.identifier}__f__"),
        type=RelationType.Table,
    )

    results = _write_relation(data, project_dir, profiles_dir, temporal_relation, dtype)
    try:
        _replace_relation(project_dir, profiles_dir, relation, temporal_relation)

        return results
    except:
        _drop_relation(project_dir, profiles_dir, temporal_relation)
        raise


def write_target(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    target: Union[ParsedModelNode, ParsedSourceDefinition],
    dtype=None,
) -> RemoteRunResult:
    relation: BaseRelation = _get_target_relation(  # type: ignore
        target, project_dir, profiles_dir, default=True
    )
    return _write_relation(data, project_dir, profiles_dir, relation, dtype)


def _write_relation(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    relation: BaseRelation,
    dtype=None,
) -> RemoteRunResult:
    adapter = _get_adapter(project_dir, profiles_dir)

    engine = _alchemy_engine(adapter, relation.database)
    pddb = pdsql.SQLDatabase(engine, schema=relation.schema)
    pdtable = pdsql.SQLTable(relation.identifier, pddb, data, index=False, dtype=dtype)
    alchemy_table: sqlalchemy.Table = pdtable.table.to_metadata(pdtable.pd_sql.meta)

    column_names: List[str] = list(data.columns)

    rows = data.to_records(index=False)
    row_dicts = list(map(lambda row: dict(zip(column_names, row)), rows))

    create_stmt = CreateTable(alchemy_table, if_not_exists=True).compile(
        bind=engine, compile_kwargs={"literal_binds": True}
    )

    _execute_sql(project_dir, profiles_dir, six.text_type(create_stmt).strip())
    _clean_cache(project_dir, profiles_dir)

    insert_stmt = Insert(alchemy_table, values=row_dicts).compile(
        bind=engine, compile_kwargs={"literal_binds": True}
    )

    _, result = _execute_sql(
        project_dir, profiles_dir, six.text_type(insert_stmt).strip()
    )
    return result


# HACK: we are cleaning the cache because if we dropped or renamed a table with an empty cache,
#       it populates it with that single table
def _clean_cache(project_dir: str, profiles_dir: str):
    adapter = _get_adapter(project_dir, profiles_dir)
    config = parse.get_dbt_config(project_dir, profiles_dir)
    manifest = parse.get_dbt_manifest(config)

    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "clean_cache:" + str(uuid4())
    with adapter.connection_named(name):
        adapter.set_relations_cache(manifest, True)


def _replace_relation(
    project_dir: str,
    profiles_dir: str,
    original_relation: BaseRelation,
    new_relation: BaseRelation,
):
    adapter = _get_adapter(project_dir, profiles_dir)

    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "replace_relation:" + str(hash(str(original_relation))) + ":" + str(uuid4())
    with adapter.connection_named(name):
        adapter.connections.begin()
        adapter.drop_relation(original_relation)
        adapter.rename_relation(new_relation, original_relation)
        adapter.connections.commit_if_has_connection()
    _clean_cache(project_dir, profiles_dir)


def _drop_relation(
    project_dir: str,
    profiles_dir: str,
    relation: BaseRelation,
):
    adapter = _get_adapter(project_dir, profiles_dir)

    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "drop_relation:" + str(hash(str(relation))) + ":" + str(uuid4())
    with adapter.connection_named(name):
        adapter.connections.begin()
        adapter.drop_relation(relation)
        adapter.connections.commit_if_has_connection()
    _clean_cache(project_dir, profiles_dir)


def _alchemy_engine(
    adapter: SQLAdapter,
    database: Optional[str],
):
    url_string = f"{adapter.type()}://"
    if adapter.type() == "bigquery":
        assert database is not None
        url_string = f"bigquery://{database}"
    if adapter.type() == "postgres":
        url_string = "postgresql://"
    # TODO: add special cases as needed

    def null_dump(sql, *multiparams, **params):
        pass

    return sqlalchemy.create_mock_engine(url_string, executor=null_dump)
