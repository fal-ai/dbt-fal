# NOTE: INSPIRED IN https://github.com/dbt-labs/dbt-core/blob/43edc887f97e359b02b6317a9f91898d3d66652b/core/dbt/lib.py
from contextlib import contextmanager
import six
from enum import Enum
from dataclasses import dataclass
from uuid import uuid4
from typing import Iterator, List, Optional, Tuple
from urllib.parse import quote_plus
import threading

import dbt.version
from dbt.semver import VersionSpecifier
import dbt.flags as flags
import dbt.adapters.factory as adapters_factory
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base import BaseRelation, BaseAdapter, BaseConnectionManager
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.config import RuntimeConfig

from . import parse

import pandas as pd
from pandas.io import sql as pdsql

import agate
import sqlalchemy
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql import Insert


_DBT_V1 = VersionSpecifier.from_version_string("1.0.0")
DBT_VCURRENT = dbt.version.installed

IS_DBT_V1PLUS = DBT_VCURRENT.compare(_DBT_V1) >= 0

from dbt.contracts.sql import RemoteRunResult


class WriteModeEnum(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


@dataclass
class FlagsArgs:
    profiles_dir: str
    use_colors: Optional[bool]


def initialize_dbt_flags(profiles_dir: str):
    """
    Initializes the flags module from dbt, since it's accessed from around their code.
    """
    args = FlagsArgs(profiles_dir, None)
    user_config = parse.get_dbt_user_config(profiles_dir)

    flags.set_from_args(args, user_config)

    # Set invocation id
    import dbt.events.functions as events_functions

    events_functions.set_invocation_id()

    # Re-enable logging for 1.0.0 through old API of logger
    # TODO: migrate for 1.0.0 code to new event system
    flags.ENABLE_LEGACY_LOGGER = "1"


def register_adapters(config: RuntimeConfig):
    # HACK: to avoid 'Node package named <profile> not found'
    adapters_factory.reset_adapters()
    adapters_factory.register_adapter(config)


# NOTE: Once we get an adapter, we must call `connection_for` or `connection_named` to use it
def _get_adapter(
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    *,
    config: Optional[RuntimeConfig] = None,
) -> SQLAdapter:
    if config is None:
        config = parse.get_dbt_config(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            profile_target=profile_target,
        )
    adapter: SQLAdapter = adapters_factory.get_adapter(config)  # type: ignore

    return adapter


global _lock
# RLock supports recursive locking by the same thread
_lock = threading.RLock()


@contextmanager
def _cache_lock(info: str = ""):
    operationId = uuid4()
    logger.debug("Locking  {} {}", operationId, info)

    _lock.acquire()
    logger.debug("Acquired {}", operationId)

    try:
        yield
    except:
        logger.debug("Error during lock operation {}", operationId)
        raise
    finally:
        _lock.release()
        logger.debug("Released {}", operationId)


def _connection_name(prefix: str, obj, _hash: bool = True):
    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    return f"{prefix}:{hash(str(obj)) if _hash else obj}:{uuid4()}"


def _execute_sql(
    project_dir: str,
    profiles_dir: str,
    sql: str,
    profile_target: str,
    *,
    config: Optional[RuntimeConfig] = None,
    adapter: Optional[SQLAdapter] = None,
) -> Tuple[AdapterResponse, pd.DataFrame]:
    open_conn = adapter is None
    if adapter is None:
        adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    if adapter.type() == "bigquery":
        return _bigquery_execute_sql(adapter, sql, open_conn)

    with _existing_or_new_connection(
        adapter, _connection_name("execute_sql", sql), open_conn
    ) as is_new:
        exec_response: Tuple[AdapterResponse, agate.Table] = adapter.execute(
            sql, auto_begin=is_new, fetch=True
        )
        response, agate_table = exec_response

        if is_new:
            adapter.commit_if_has_connection()

    return response, _agate_table_to_df(agate_table)


def _clear_relations_cache(adapter: BaseAdapter, config: RuntimeConfig):
    # HACK: Sometimes we cache an incomplete cache or create stuff without the cache noticing.
    #       Some adapters work without this. We should separate adapter solutions like dbt.
    manifest = parse.get_dbt_manifest(config)
    adapter.set_relations_cache(manifest, True)


def _get_target_relation(
    target: CompileResultNode,
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
) -> Optional[BaseRelation]:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)
    config = parse.get_dbt_config(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile_target=profile_target,
    )

    with adapter.connection_named(_connection_name("relation", target)):
        with _cache_lock("_get_target_relation"):
            _clear_relations_cache(adapter, config)

            # This ROLLBACKs so it has to be a new connection
            return adapter.get_relation(
                target.database, target.schema, target.identifier
            )


def compile_sql(
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    sql: str,
    *,
    config: Optional[RuntimeConfig] = None,
):
    from dbt.parser.manifest import process_node
    from dbt.task.sql import SqlCompileRunner
    from dbt.parser.sql import SqlBlockParser

    if config is None:
        config = parse.get_dbt_config(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            profile_target=profile_target,
        )

    manifest = parse.get_dbt_manifest(config)

    adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    block_parser = SqlBlockParser(
        project=config,
        manifest=manifest,
        root_project=config,
    )

    sql_node = block_parser.parse_remote(sql, _connection_name("compile_sql", sql))
    process_node(config, manifest, sql_node)
    runner = SqlCompileRunner(config, adapter, sql_node, 1, 1)
    result: RemoteRunResult = runner.safe_run(manifest)
    return result


def execute_sql(
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    sql: str,
    *,
    config: Optional[RuntimeConfig] = None,
) -> pd.DataFrame:
    return _execute_sql(
        project_dir, profiles_dir, sql, profile_target=profile_target, config=config
    )[1]


def _agate_table_to_df(table: agate.Table) -> pd.DataFrame:
    column_names = list(table.column_names)
    rows = [list(row) for row in table]

    # TODO: better type matching?
    return pd.DataFrame.from_records(data=rows, columns=column_names, coerce_float=True)


def fetch_target(
    project_dir: str,
    profiles_dir: str,
    target: CompileResultNode,
    profile_target: str,
) -> pd.DataFrame:
    relation = _get_target_relation(
        target, project_dir, profiles_dir, profile_target=profile_target
    )

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    return _fetch_relation(project_dir, profiles_dir, profile_target, relation)


def _fetch_relation(
    project_dir: str, profiles_dir: str, profile_target: str, relation: BaseRelation
) -> pd.DataFrame:
    query = f"SELECT * FROM {relation}"
    _, df = _execute_sql(
        project_dir, profiles_dir, query, profile_target=profile_target
    )
    return df


def _build_table_from_parts(
    adapter: SQLAdapter,
    database: Optional[str],
    schema: Optional[str],
    identifier: Optional[str],
) -> BaseRelation:
    from dbt.contracts.relation import Path, RelationType

    if adapter.type() == "snowflake":
        if database is not None:
            database = database.lower()
        if schema is not None:
            schema = schema.lower()
        if identifier is not None:
            identifier = identifier.lower()

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
    profile_target: str,
    target: CompileResultNode,
    *,
    dtype=None,
) -> AdapterResponse:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    relation = _build_table_from_target(adapter, target)

    if adapter.type() == "bigquery":
        return _bigquery_write_relation(
            data,
            project_dir,
            profiles_dir,
            profile_target,
            relation,
            mode=WriteModeEnum.OVERWRITE,
            fields_schema=dtype,
        )

    # With some writing functions, it could be called twice at the same time for the same identifier
    # so we avoid overwriting temporal tables by attaching uniqueness to the name
    unique_str = str(uuid4())[0:8]
    temporal_relation = _build_table_from_parts(
        adapter,
        relation.database,
        relation.schema,
        f"{relation.identifier}__f__{unique_str}",
    )

    results = _write_relation(
        data,
        project_dir,
        profiles_dir,
        profile_target,
        temporal_relation,
        dtype=dtype,
    )
    try:
        _replace_relation(
            project_dir,
            profiles_dir,
            profile_target,
            relation,
            temporal_relation,
        )

        return results
    except:
        _drop_relation(
            project_dir,
            profiles_dir,
            temporal_relation,
            profile_target=profile_target,
        )
        raise


def write_target(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    target: CompileResultNode,
    *,
    dtype=None,
) -> AdapterResponse:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    relation = _build_table_from_target(adapter, target)

    if adapter.type() == "bigquery":
        return _bigquery_write_relation(
            data,
            project_dir,
            profiles_dir,
            profile_target,
            relation,
            mode=WriteModeEnum.APPEND,
            fields_schema=dtype,
        )

    return _write_relation(
        data, project_dir, profiles_dir, profile_target, relation, dtype=dtype
    )


def _write_relation(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    relation: BaseRelation,
    *,
    dtype=None,
) -> AdapterResponse:
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)

    assert adapter.type() != "bigquery", "Should not have reached here with bigquery"

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

    response, _ = _execute_sql(
        project_dir,
        profiles_dir,
        six.text_type(insert_stmt).strip(),
        profile_target=profile_target,
    )
    return response


def _replace_relation(
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    original_relation: BaseRelation,
    new_relation: BaseRelation,
):
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)
    config = parse.get_dbt_config(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile_target=profile_target,
    )

    with adapter.connection_named(
        _connection_name("replace_relation", original_relation, _hash=False)
    ):
        with _cache_lock("_replace_relation"):
            adapter.connections.begin()

            _clear_relations_cache(adapter, config)

            if adapter.type() not in ("bigquery", "snowflake"):
                # This is a 'DROP ... IF EXISTS', so it always works
                adapter.drop_relation(original_relation)

            if adapter.type() == "athena":
                # HACK: athena doesn't support renaming tables, we do it manually
                create_stmt = f"create table {original_relation} as select * from {new_relation} with data"
                _execute_sql(
                    project_dir,
                    profiles_dir,
                    six.text_type(create_stmt).strip(),
                    profile_target=profile_target,
                    adapter=adapter,
                )
                adapter.drop_relation(new_relation)
            elif adapter.type() == "bigquery":
                create_stmt = f"create or replace table {original_relation} as select * from {new_relation}"
                _execute_sql(
                    project_dir,
                    profiles_dir,
                    six.text_type(create_stmt).strip(),
                    profile_target=profile_target,
                    adapter=adapter,
                )
                adapter.drop_relation(new_relation)
            elif adapter.type() == "snowflake":
                create_stmt = (
                    f"create or replace table {original_relation} clone {new_relation}"
                )
                _execute_sql(
                    project_dir,
                    profiles_dir,
                    six.text_type(create_stmt).strip(),
                    profile_target=profile_target,
                    adapter=adapter,
                )
                adapter.drop_relation(new_relation)
            else:
                adapter.rename_relation(new_relation, original_relation)

            adapter.commit_if_has_connection()


def _drop_relation(
    project_dir: str,
    profiles_dir: str,
    relation: BaseRelation,
    profile_target: str,
):
    adapter = _get_adapter(project_dir, profiles_dir, profile_target)
    config = parse.get_dbt_config(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        profile_target=profile_target,
    )

    with adapter.connection_named(_connection_name("drop_relation", relation)):
        with _cache_lock("_drop_relation"):
            adapter.connections.begin()

            _clear_relations_cache(adapter, config)

            adapter.drop_relation(relation)

            adapter.commit_if_has_connection()


def _alchemy_engine(adapter: SQLAdapter, database: Optional[str]):
    url_string = f"{adapter.type()}://"
    if adapter.type() == "bigquery":
        assert database is not None
        url_string = f"bigquery://{database}"

    if adapter.type() == "postgres":
        url_string = "postgresql://"

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

    # TODO: add special cases as needed

    def null_dump(sql, *multiparams, **params):
        pass

    return sqlalchemy.create_mock_engine(url_string, executor=null_dump)


@contextmanager
def _existing_or_new_connection(
    adapter: BaseAdapter,
    name: str,
    open_conn: bool,  # TODO: open_conn solution feels hacky
) -> Iterator[bool]:
    if open_conn:
        with adapter.connection_named(name):
            yield True
    else:
        yield False


# Adapter: BigQuery
def _bigquery_execute_sql(
    adapter: BaseAdapter, sql: str, open_conn: bool
) -> Tuple[AdapterResponse, pd.DataFrame]:
    assert adapter.type() == "bigquery"

    import google.cloud.bigquery as bigquery

    with _existing_or_new_connection(
        adapter, _connection_name("bigquery:execute_sql", sql), open_conn
    ):
        conection_manager: BaseConnectionManager = adapter.connections  # type: ignore
        conn = conection_manager.get_thread_connection()
        client: bigquery.Client = conn.handle  # type: ignore

        job = client.query(sql)
        df = job.to_dataframe()
        if job.destination:
            query_table = client.get_table(job.destination)
            num_rows = query_table.num_rows
        else:
            num_rows = df.size

    # TODO: better AdapterResponse
    return AdapterResponse("OK", rows_affected=num_rows), df


def _bigquery_write_relation(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    relation: BaseRelation,
    *,
    mode: WriteModeEnum,
    fields_schema: Optional[List[dict]] = None,
) -> AdapterResponse:
    import google.cloud.bigquery as bigquery
    from google.cloud.bigquery.job import WriteDisposition
    from dbt.adapters.bigquery import BigQueryAdapter, BigQueryConnectionManager

    adapter: BigQueryAdapter = _get_adapter(project_dir, profiles_dir, profile_target)  # type: ignore
    assert adapter.type() == "bigquery"

    disposition = (
        WriteDisposition.WRITE_TRUNCATE
        if WriteModeEnum.OVERWRITE == mode
        else WriteDisposition.WRITE_APPEND
    )

    project: str = relation.database  # type: ignore
    dataset: str = relation.schema  # type: ignore
    table: str = relation.identifier  # type: ignore

    with adapter.connection_named(
        _connection_name("bigquery:write_relation", relation, _hash=False)
    ):
        connection_manager: BigQueryConnectionManager = adapter.connections
        conn = connection_manager.get_thread_connection()
        client: bigquery.Client = conn.handle  # type: ignore

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project, dataset), table
        )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=[
                # field_types is a list of API-representation of BigQuery.FieldSchema
                bigquery.SchemaField.from_api_repr(field)
                for field in (fields_schema or [])
            ],
            source_format="PARQUET",
            write_disposition=disposition,
        )

        with connection_manager.exception_handler("START JOB"):
            job = client.load_table_from_dataframe(
                data, table_ref, job_config=job_config
            )

        from dbt.adapters.bigquery.__version__ import version as bigquery_version

        ADAPTER_VCURRENT = VersionSpecifier.from_version_string(bigquery_version)
        # https://github.com/dbt-labs/dbt-bigquery/commit/141b86749df813cf3a3a90a90e7a7dfc401ba9b0
        if ADAPTER_VCURRENT.compare(VersionSpecifier.from_version_string("1.1.0")) >= 0:
            timeout = connection_manager.get_job_execution_timeout_seconds(conn) or 300
        else:
            timeout = connection_manager.get_timeout(conn) or 300

        with connection_manager.exception_handler("LOAD TABLE"):
            adapter.poll_until_job_completes(job, timeout)

        query_table = client.get_table(job.destination)
        num_rows = query_table.num_rows

    # TODO: better AdapterResponse
    return AdapterResponse("OK", rows_affected=num_rows)
