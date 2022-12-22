# NOTE: INSPIRED IN https://github.com/dbt-labs/dbt-core/blob/43edc887f97e359b02b6317a9f91898d3d66652b/core/dbt/lib.py
from contextlib import contextmanager
import six
from enum import Enum
from dataclasses import dataclass
from uuid import uuid4
from typing import Iterator, List, Optional, Tuple
from urllib.parse import quote_plus
import threading

import dbt.flags as flags
import dbt.adapters.factory as adapters_factory

from dbt.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base import BaseRelation, BaseAdapter, BaseConnectionManager
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.config import RuntimeConfig

import pandas as pd
from pandas.io import sql as pdsql

import agate
import sqlalchemy
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql import Insert

from dbt.contracts.sql import RemoteRunResult

from faldbt import parse
from faldbt.logger import LOGGER


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
    LOGGER.debug("Locking  {} {}", operationId, info)

    _lock.acquire()
    LOGGER.debug("Acquired {}", operationId)

    try:
        yield
    except:
        LOGGER.debug("Error during lock operation {}", operationId)
        raise
    finally:
        _lock.release()
        LOGGER.debug("Released {}", operationId)


def _connection_name(prefix: str, obj, _hash: bool = True):
    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    return f"{prefix}:{hash(str(obj)) if _hash else obj}:{uuid4()}"


def _execute_sql(
    adapter: SQLAdapter,
    sql: str,
    *,
    new_conn=True,
) -> Tuple[AdapterResponse, pd.DataFrame]:

    if adapter.type() == "bigquery":
        return _bigquery_execute_sql(adapter, sql, new_conn)

    if adapter.type() == "snowflake":
        return _snowflake_execute_sql(adapter, sql, new_conn)

    with _existing_or_new_connection(
        adapter, _connection_name("execute_sql", sql), new_conn
    ) as is_new:
        exec_response: Tuple[AdapterResponse, agate.Table] = adapter.execute(
            sql, auto_begin=is_new, fetch=True
        )
        response, agate_table = exec_response

        if is_new:
            adapter.commit_if_has_connection()

    return response, _agate_table_to_df(agate_table)


def _clear_relations_cache(adapter: BaseAdapter):
    # HACK: Sometimes we cache an incomplete cache or create stuff without the cache noticing.
    #       Some adapters work without this. We should separate adapter solutions like dbt.
    manifest = parse.get_dbt_manifest(adapter.config)
    adapter.set_relations_cache(manifest, True)


def _get_target_relation(
    adapter: SQLAdapter, target: CompileResultNode
) -> Optional[BaseRelation]:
    with adapter.connection_named(_connection_name("relation", target)):
        with _cache_lock("_get_target_relation"):
            _clear_relations_cache(adapter)

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
    adapter: Optional[SQLAdapter] = None,
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

    if adapter is None:
        adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    manifest = parse.get_dbt_manifest(config)

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
    adapter: Optional[SQLAdapter] = None,
) -> pd.DataFrame:
    if adapter is None:
        adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    return _execute_sql(adapter, sql)[1]


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
    *,
    config: Optional[RuntimeConfig] = None,
    adapter: Optional[SQLAdapter] = None,
) -> pd.DataFrame:
    if adapter is None:
        adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    relation = _get_target_relation(adapter, target)

    if relation is None:
        raise Exception(f"Could not get relation for '{target.unique_id}'")

    return _fetch_relation(adapter, relation)


def _fetch_relation(adapter: SQLAdapter, relation: BaseRelation) -> pd.DataFrame:
    if adapter.type() == "postgres":
        return _sqlalchemy_engine_fetch_relation(adapter, relation)

    query = f"SELECT * FROM {relation}"
    return _execute_sql(adapter, query)[1]


def _build_table_from_parts(
    adapter: SQLAdapter,
    database: Optional[str],
    schema: Optional[str],
    identifier: Optional[str],
) -> BaseRelation:
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
    profile_target: str,
    target: CompileResultNode,
    *,
    dtype=None,
    config: Optional[RuntimeConfig] = None,
    adapter: Optional[SQLAdapter] = None,
) -> AdapterResponse:
    if not adapter:
        adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    relation = _build_table_from_target(adapter, target)

    if adapter.type() == "bigquery":
        return _bigquery_write_relation(
            adapter,
            data,
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

    results = _write_relation(adapter, data, temporal_relation, dtype=dtype)

    try:
        _replace_relation(adapter, relation, temporal_relation)

        return results
    except:
        _drop_relation(adapter, temporal_relation)
        raise


def write_target(
    data: pd.DataFrame,
    project_dir: str,
    profiles_dir: str,
    profile_target: str,
    target: CompileResultNode,
    *,
    dtype=None,
    config: Optional[RuntimeConfig] = None,
    adapter: Optional[SQLAdapter] = None,
) -> AdapterResponse:
    if adapter is None:
        adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)

    relation = _build_table_from_target(adapter, target)

    return _write_relation(adapter, data, relation, dtype=dtype)


def _write_relation(
    adapter: SQLAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
    *,
    dtype=None,
) -> AdapterResponse:
    if adapter.type() == "fal":
        adapter = adapter._db_adapter

    if adapter.type() == "bigquery":
        return _bigquery_write_relation(
            adapter,
            data,
            relation,
            mode=WriteModeEnum.APPEND,
            fields_schema=dtype,
        )

    if adapter.type() == "snowflake":
        return _snowflake_write_relation(
            adapter,
            data,
            relation,
        )

    if adapter.type() == "postgres":
        return _sqlalchemy_engine_write_relation(adapter, data, relation, dtype=dtype)

    database, schema, identifier = (
        relation.database,
        relation.schema,
        relation.identifier,
    )

    engine = _alchemy_mock_engine(adapter)
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
    _execute_sql(adapter, six.text_type(create_stmt).strip())

    insert_stmt = Insert(alchemy_table, values=row_dicts).compile(
        bind=engine, compile_kwargs={"literal_binds": True}
    )
    response, _ = _execute_sql(adapter, six.text_type(insert_stmt).strip())
    return response


def _replace_relation(
    adapter: SQLAdapter,
    original_relation: BaseRelation,
    new_relation: BaseRelation,
):
    with adapter.connection_named(
        _connection_name("replace_relation", original_relation, _hash=False)
    ):
        with _cache_lock("_replace_relation"):
            adapter.connections.begin()

            _clear_relations_cache(adapter)

            if adapter.type() not in ("bigquery", "snowflake"):
                # This is a 'DROP ... IF EXISTS', so it always works
                adapter.drop_relation(original_relation)

            if adapter.type() == "athena":
                # HACK: athena doesn't support renaming tables, we do it manually
                create_stmt = f"create table {original_relation} as select * from {new_relation} with data"
                _execute_sql(
                    adapter,
                    six.text_type(create_stmt).strip(),
                    new_conn=False,
                )
                adapter.drop_relation(new_relation)
            elif adapter.type() == "bigquery":
                create_stmt = f"create or replace table {original_relation} as select * from {new_relation}"
                _bigquery_execute_sql(
                    adapter,
                    six.text_type(create_stmt).strip(),
                    new_conn=False,
                )
                adapter.drop_relation(new_relation)
            elif adapter.type() == "snowflake":
                create_stmt = (
                    f"create or replace table {original_relation} clone {new_relation}"
                )
                _snowflake_execute_sql(
                    adapter=adapter,
                    sql=six.text_type(create_stmt).strip(),
                    new_conn=False,
                    fetch=False,  # Avoid trying to fetch as pandas
                )
                adapter.drop_relation(new_relation)
            else:
                adapter.rename_relation(new_relation, original_relation)

            adapter.commit_if_has_connection()


def _drop_relation(adapter: SQLAdapter, relation: BaseRelation):
    with adapter.connection_named(_connection_name("drop_relation", relation)):
        with _cache_lock("_drop_relation"):
            adapter.connections.begin()

            _clear_relations_cache(adapter)

            adapter.drop_relation(relation)

            adapter.commit_if_has_connection()


def _alchemy_mock_engine(adapter: SQLAdapter):
    url_string = f"{adapter.type()}://"
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


def _create_engine_from_connection(adapter: SQLAdapter):
    if adapter.type() == "postgres":
        url_string = "postgresql+psycopg2://"
    else:
        # TODO: add special cases as needed
        LOGGER.warn("No explicit url string for adapter {}", adapter.type())
        url_string = f"{adapter.type()}://"

    connection = adapter.connections.get_thread_connection()
    return sqlalchemy.create_engine(url_string, creator=lambda: connection.handle)


@contextmanager
def _existing_or_new_connection(
    adapter: BaseAdapter,
    name: str,
    new_conn: bool,  # TODO: new_conn solution feels hacky
) -> Iterator[bool]:
    if new_conn:
        with adapter.connection_named(name):
            yield True
    else:
        yield False


# Adapter: salalchemy connection
def _sqlalchemy_engine_fetch_relation(adapter: SQLAdapter, relation: BaseRelation):
    # TODO: use database, just using schema and identifier
    database, schema, identifier = (
        relation.database,
        relation.schema,
        relation.identifier,
    )

    assert identifier

    with _existing_or_new_connection(
        adapter, _connection_name("write_target", relation, _hash=False), True
    ):
        engine = _create_engine_from_connection(adapter)
        # TODO: use database, just using schema and identifier
        return pd.read_sql_table(
            table_name=identifier,
            schema=schema,
            con=engine,
        )


def _sqlalchemy_engine_write_relation(
    adapter: SQLAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
    *,
    dtype=None,
):
    # TODO: use database, just using schema and identifier
    database, schema, identifier = (
        relation.database,
        relation.schema,
        relation.identifier,
    )

    assert identifier

    with _existing_or_new_connection(
        adapter, _connection_name("write_target", relation, _hash=False), True
    ):
        engine = _create_engine_from_connection(adapter)

        rows_affected = data.to_sql(
            name=identifier,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype=dtype,
        )

        return AdapterResponse("OK", rows_affected=rows_affected)


# Adapter: BigQuery
def _bigquery_execute_sql(
    adapter: BaseAdapter, sql: str, new_conn: bool
) -> Tuple[AdapterResponse, pd.DataFrame]:
    assert adapter.type() == "bigquery"

    import google.cloud.bigquery as bigquery

    with _existing_or_new_connection(
        adapter, _connection_name("bigquery:execute_sql", sql), new_conn
    ):
        connection_manager: BaseConnectionManager = adapter.connections  # type: ignore
        client: bigquery.Client = connection_manager.get_thread_connection().handle  # type: ignore

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
    adapter: SQLAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
    *,
    mode: WriteModeEnum,
    fields_schema: Optional[List[dict]] = None,
) -> AdapterResponse:
    import google.cloud.bigquery as bigquery
    from google.cloud.bigquery.job import WriteDisposition
    from dbt.adapters.bigquery import BigQueryAdapter, BigQueryConnectionManager
    from dbt.semver import VersionSpecifier

    assert adapter.type() == "bigquery"

    _adapter: BigQueryAdapter = adapter  # type: ignore

    disposition = (
        WriteDisposition.WRITE_TRUNCATE
        if WriteModeEnum.OVERWRITE == mode
        else WriteDisposition.WRITE_APPEND
    )

    project: str = relation.database  # type: ignore
    dataset: str = relation.schema  # type: ignore
    table: str = relation.identifier  # type: ignore

    with _adapter.connection_named(
        _connection_name("bigquery:write_relation", relation, _hash=False)
    ):
        connection_manager: BigQueryConnectionManager = _adapter.connections
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
            _adapter.poll_until_job_completes(job, timeout)

        query_table = client.get_table(job.destination)
        num_rows = query_table.num_rows

    # TODO: better AdapterResponse
    return AdapterResponse("OK", rows_affected=num_rows)


# Adapter: Snowflake
def _snowflake_execute_sql(
    adapter: BaseAdapter,
    sql: str,
    new_conn: bool,
    *,
    fetch: bool = True,
) -> Tuple[AdapterResponse, pd.DataFrame]:
    assert adapter.type() == "snowflake"

    import snowflake.connector as snowflake
    from dbt.adapters.snowflake import SnowflakeConnectionManager

    with _existing_or_new_connection(
        adapter, _connection_name("snowflake:execute_sql", sql), new_conn
    ):
        connection_manager: SnowflakeConnectionManager = adapter.connections  # type: ignore
        conn: snowflake.SnowflakeConnection = connection_manager.get_thread_connection().handle  # type: ignore

        with connection_manager.exception_handler("EXECUTE SQL"):
            cur = conn.cursor()

            cur.execute(sql)

            # Use snowflake-dbt function directly
            res = connection_manager.get_response(cur)

            df = pd.DataFrame({})
            if fetch:
                df: pd.DataFrame = cur.fetch_pandas_all()

                # HACK: manually parse ARRAY and VARIANT since they are returned as strings right now
                # Related issue: https://github.com/snowflakedb/snowflake-connector-python/issues/544
                for desc in cur.description:
                    # 5=VARIANT, 10=ARRAY -- https://docs.snowflake.com/en/user-guide/python-connector-api.html#type-codes
                    if desc.type_code in [5, 10]:
                        import json

                        df[desc.name] = df[desc.name].map(lambda v: json.loads(v))

    return res, df


def _snowflake_write_relation(
    adapter: SQLAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
) -> AdapterResponse:
    from dbt.adapters.snowflake import SnowflakeAdapter, SnowflakeConnectionManager
    import snowflake.connector as snowflake
    import snowflake.connector.pandas_tools as snowflake_pandas

    assert adapter.type() == "snowflake"

    _adapter: SnowflakeAdapter = adapter  # type: ignore

    database: str = relation.database  # type: ignore
    schema: str = relation.schema  # type: ignore
    table: str = relation.identifier  # type: ignore

    with _adapter.connection_named(
        _connection_name("snowflake:write_relation", relation, _hash=False)
    ):
        connection_manager: SnowflakeConnectionManager = _adapter.connections  # type: ignore
        conn: snowflake.SnowflakeConnection = connection_manager.get_thread_connection().handle  # type: ignore

        with connection_manager.exception_handler("LOAD TABLE"):
            success, chunks, num_rows, output = snowflake_pandas.write_pandas(
                conn,
                data,
                table_name=table,
                database=database,
                schema=schema,
                auto_create_table=True,
                quote_identifiers=False,
            )
            if not success:
                # In case the failure does not raise by itself
                # I have not been able to reproduce such a case
                from dbt.exceptions import DatabaseException

                raise DatabaseException(output)

    # TODO: better AdapterResponse
    return AdapterResponse(str(output), rows_affected=num_rows)
