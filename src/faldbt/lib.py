# NOTE: INSPIRED IN https://github.com/dbt-labs/dbt-core/blob/43edc887f97e359b02b6317a9f91898d3d66652b/core/dbt/lib.py
from time import sleep
import six
import os
from datetime import datetime
from dataclasses import dataclass
from uuid import uuid4
from typing import List, Optional, Tuple
from urllib.parse import quote_plus

import dbt.version
import dbt.semver
import dbt.flags as flags
import dbt.adapters.factory as adapters_factory
from dbt.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base import BaseRelation
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.parsed import ParsedModelNode

from . import parse

import pandas as pd
from pandas.io import sql as pdsql

import sqlalchemy
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql import Insert

DBT_V1 = dbt.semver.VersionSpecifier.from_version_string("1.0.0")
DBT_VCURRENT = dbt.version.get_installed_version()

if DBT_VCURRENT.compare(DBT_V1) >= 0:
    from dbt.contracts.sql import ResultTable, RemoteRunResult
else:
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
def _get_adapter(
    project_dir: str, profiles_dir: str, profile_target: str
) -> SQLAdapter:
    config = parse.get_dbt_config(
        project_dir, profiles_dir, profile_target=profile_target
    )
    adapter: SQLAdapter = adapters_factory.get_adapter(config)  # type: ignore

    return adapter


def _execute_sql(
    project_dir: str, profiles_dir: str, sql: str, profile_target: str = None
) -> Tuple[AdapterResponse, RemoteRunResult]:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

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
    target: CompileResultNode,
    project_dir: str,
    profiles_dir: str,
    profile_target: str = None,
) -> Optional[BaseRelation]:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)
    config = parse.get_dbt_config(
        project_dir, profiles_dir, profile_target=profile_target
    )
    manifest = parse.get_dbt_manifest(config)

    if adapter.type() == "bigquery":
        # After creating a table, BigQuery takes some time to realize it is there
        sleep(2)

    name = "relation:" + str(hash(str(target))) + ":" + str(uuid4())
    relation = None
    with adapter.connection_named(name):
        # HACK: Sometimes we cache an incomplete cache or create stuff without the cache noticing.
        #       Some adapters work without this. We should separate adapter solutions like dbt.
        adapter.set_relations_cache(manifest, True)
        target_name = target.name
        if isinstance(target, ParsedModelNode):
            target_name = target.alias

        # This ROLLBACKs so it has to be a new connection
        relation = adapter.get_relation(target.database, target.schema, target_name)

    return relation


def execute_sql(
    project_dir: str, profiles_dir: str, sql: str, profile_target: str = None
) -> RemoteRunResult:
    _, result = _execute_sql(
        project_dir, profiles_dir, sql, profile_target=profile_target
    )
    return result


def fetch_target(
    project_dir: str, profiles_dir: str, target: CompileResultNode, profile_target=None
) -> RemoteRunResult:
    relation = _get_target_relation(
        target, project_dir, profiles_dir, profile_target=profile_target
    )

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    query = f"SELECT * FROM {relation}"
    _, result = _execute_sql(
        project_dir, profiles_dir, query, profile_target=profile_target
    )
    return result


def _build_table_from_parts(
    adapter: SQLAdapter,
    database: Optional[str],
    schema: Optional[str],
    identifier: Optional[str],
):
    from dbt.contracts.relation import Path, RelationType

    path = Path(database, schema, identifier)

    # NOTE: assuming we want TABLE relation if not found
    return adapter.Relation(path, type=RelationType.Table)


def _build_table_from_target(adapter: SQLAdapter, target: CompileResultNode):
    return _build_table_from_parts(
        adapter, target.database, target.schema, target.identifier
    )


def overwrite_target(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    target: CompileResultNode,
    dtype=None,
    profile_target: str = None,
) -> RemoteRunResult:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    relation = _get_target_relation(
        target, project_dir, profiles_dir, profile_target=profile_target
    )
    if relation is None:
        relation = _build_table_from_target(adapter, target)

    temporal_relation = _build_table_from_parts(
        adapter, relation.database, relation.schema, f"{relation.identifier}__f__"
    )

    results = _write_relation(
        data,
        project_dir,
        profiles_dir,
        temporal_relation,
        dtype,
        profile_target=profile_target,
    )
    try:
        _replace_relation(
            project_dir,
            profiles_dir,
            relation,
            temporal_relation,
            profile_target=profile_target,
        )

        return results
    except:
        _drop_relation(
            project_dir, profiles_dir, temporal_relation, profile_target=profile_target
        )
        raise


def write_target(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    target: CompileResultNode,
    dtype=None,
    profile_target: str = None,
) -> RemoteRunResult:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    relation = _get_target_relation(
        target, project_dir, profiles_dir, profile_target=profile_target
    )
    if relation is None:
        relation = _build_table_from_target(adapter, target)

    return _write_relation(
        data, project_dir, profiles_dir, relation, dtype, profile_target=profile_target
    )


def _write_relation(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    relation: BaseRelation,
    dtype=None,
    profile_target: str = None,
) -> RemoteRunResult:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    if adapter.type() == "snowflake":
        database, schema, identifier = (
            relation.database.lower(),
            relation.schema.lower(),
            relation.identifier.lower(),
        )
    else:
        database, schema, identifier = (
            relation.database,
            relation.schema,
            relation.identifier,
        )

    engine = _alchemy_engine(adapter, database)
    pddb = pdsql.SQLDatabase(engine, schema=schema)
    pdtable = pdsql.SQLTable(identifier, pddb, data, index=False, dtype=dtype)

    alchemy_table: sqlalchemy.Table = pdtable.table.to_metadata(pdtable.pd_sql.meta)

    # HACK: athena needs "location" property that is not passed by mock adapter
    if adapter.type() == "athena":
        s3_dir = adapter.config.credentials.s3_staging_dir
        alchemy_table.dialect_options["awsathena"] = {
            "location": f"{s3_dir}{alchemy_table.schema}/{alchemy_table.name}/",
            "tblproperties": None,
            "compression": None,
            "bucket_count": None,
            "row_format": None,
            "serdeproperties": None,
            "file_format": None,
            "partition": None,
            "cluster": None,
        }

    column_names: List[str] = list(data.columns)

    rows = data.to_records(index=False)
    row_dicts = list(map(lambda row: dict(zip(column_names, row)), rows))

    create_stmt = CreateTable(alchemy_table, if_not_exists=True).compile(
        bind=engine, compile_kwargs={"literal_binds": True}
    )

    _execute_sql(
        project_dir,
        profiles_dir,
        six.text_type(create_stmt).strip(),
        profile_target=profile_target,
    )

    insert_stmt = Insert(alchemy_table, values=row_dicts).compile(
        bind=engine, compile_kwargs={"literal_binds": True}
    )

    _, result = _execute_sql(
        project_dir,
        profiles_dir,
        six.text_type(insert_stmt).strip(),
        profile_target=profile_target,
    )
    return result


def _replace_relation(
    project_dir: str,
    profiles_dir: str,
    original_relation: BaseRelation,
    new_relation: BaseRelation,
    profile_target: str = None,
):
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "replace_relation:" + str(hash(str(original_relation))) + ":" + str(uuid4())
    with adapter.connection_named(name):
        adapter.connections.begin()

        original_exists = adapter.get_relation(
            original_relation.database,
            original_relation.schema,
            original_relation.identifier,
        )
        if original_exists:
            adapter.drop_relation(original_relation)

        # HACK: athena doesn't support renaming tables, we do it manually
        if adapter.type() == "athena":
            create_stmt = f"create table {original_relation} as select * from {new_relation} with data"
            _execute_sql(
                project_dir,
                profiles_dir,
                six.text_type(create_stmt).strip(),
                profile_target=profile_target,
            )
            adapter.drop_relation(new_relation)
        else:
            adapter.rename_relation(new_relation, original_relation)
        adapter.connections.commit_if_has_connection()


def _drop_relation(
    project_dir: str,
    profiles_dir: str,
    relation: BaseRelation,
    profile_target: str = None,
):
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    name = "drop_relation:" + str(hash(str(relation))) + ":" + str(uuid4())
    with adapter.connection_named(name):
        adapter.connections.begin()
        adapter.drop_relation(relation)
        adapter.connections.commit_if_has_connection()


def _alchemy_engine(adapter: SQLAdapter, database: Optional[str]):
    url_string = f"{adapter.type()}://"
    if adapter.type() == "bigquery":
        assert database is not None
        url_string = f"bigquery://{database}"
    if adapter.type() == "postgres":
        url_string = "postgresql://"
    # TODO: add special cases as needed

    def null_dump(sql, *multiparams, **params):
        pass

    if adapter.type() == "athena":

        SCHEMA_NAME = adapter.config.credentials.schema
        S3_STAGING_DIR = adapter.config.credentials.s3_staging_dir
        AWS_REGION = adapter.config.credentials.region_name

        conn_str = (
            "awsathena+rest://athena.{region_name}.amazonaws.com:443/"
            "{schema_name}?s3_staging_dir={s3_staging_dir}&work_group=primary"
        )

        url_string = conn_str.format(
            region_name=AWS_REGION,
            schema_name=SCHEMA_NAME,
            s3_staging_dir=quote_plus(S3_STAGING_DIR),
        )

    return sqlalchemy.create_mock_engine(url_string, executor=null_dump)
