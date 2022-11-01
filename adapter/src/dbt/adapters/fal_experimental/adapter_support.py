import functools
from time import sleep
from typing import Any

import pandas as pd
import sqlalchemy
from contextlib import contextmanager
from dbt.adapters.base import BaseAdapter, BaseRelation, RelationType
from dbt.adapters.base.connections import AdapterResponse, Connection
from dbt.config import RuntimeConfig
from dbt.parser.manifest import ManifestLoader

from dbt.adapters import factory

_SQLALCHEMY_DIALECTS = {
    "postgres": "postgresql+psycopg2",
    "redshift": "redshift+psycopg2",
}


def _get_alchemy_engine(adapter: BaseAdapter, connection: Connection) -> Any:
    # The following code heavily depends on the implementation
    # details of the known adapters, hence it can't work for
    # arbitrary ones.
    adapter_type = adapter.type()

    sqlalchemy_kwargs = {}
    format_url = lambda url: url
    if adapter_type in ("postgres", "redshift", "snowflake", "duckdb"):
        # If the given adapter supports the DBAPI (PEP 249), we can
        # use its connection directly for the engine.
        sqlalchemy_kwargs["creator"] = lambda *args, **kwargs: connection.handle
        if adapter_type == "snowflake":
            format_url = (
                lambda url: url + connection.handle.host + ":" + connection.handle.port
            )
    elif adapter_type == "bigquery":
        # BigQuery's connection object returns a google-cloud
        # client, which doesn't directly support the DBAPI but
        # we can still partially leverage it to construct the
        # engine object.
        sqlalchemy_kwargs["connect_args"] = {"client": connection.handle}
        format_url = lambda url: url + "?user_supplied_client=True"
    else:
        # TODO: maybe tell them to open an issue?
        raise NotImplementedError(
            "dbt-fal does not support the given adapter " "for materializing relations."
        )

    url = _SQLALCHEMY_DIALECTS.get(adapter_type, adapter_type) + "://"
    url = format_url(url)
    return sqlalchemy.create_engine(url, **sqlalchemy_kwargs)


def drop_relation_if_it_exists(adapter: BaseAdapter, relation: BaseRelation) -> None:
    if adapter.get_relation(
        database=relation.database,
        schema=relation.schema,
        identifier=relation.identifier,
    ):
        adapter.drop_relation(relation)


def write_df_to_relation(
    adapter: BaseAdapter,
    relation: BaseRelation,
    dataframe: pd.DataFrame,
    *,
    if_exists: str = "replace",
) -> AdapterResponse:
    """Generic version of the write_df_to_relation. Materialize the given
    dataframe to the targeted relation on the adapter."""

    if adapter.type() == "snowflake":
        import dbt.adapters.fal_experimental.support.snowflake as support_snowflake

        return support_snowflake.write_df_to_relation(adapter, dataframe, relation)

    elif adapter.type() == "duckdb":
        import dbt.adapters.fal_experimental.support.duckdb as support_duckdb

        return support_duckdb.write_df_to_relation(adapter, dataframe, relation)

    else:
        with new_connection(adapter, "fal:write_df_to_relation") as connection:
            # TODO: this should probably live in the materialization macro.
            temp_relation = relation.replace_path(
                identifier=f"__dbt_fal_temp_{relation.identifier}"
            )
            drop_relation_if_it_exists(adapter, temp_relation)

            alchemy_engine = _get_alchemy_engine(adapter, connection)

            # TODO: probably worth handling errors here an returning
            # a proper adapter response.
            rows_affected = dataframe.to_sql(
                con=alchemy_engine,
                name=temp_relation.identifier,
                schema=temp_relation.schema,
                if_exists=if_exists,
                index=False,
            )
            adapter.cache.add(temp_relation)
            drop_relation_if_it_exists(adapter, relation)
            adapter.rename_relation(temp_relation, relation)
            adapter.commit_if_has_connection()

        if adapter.type() == "bigquery":
            # HACK: sleep after a bigquery write to make it propagate.
            # A real solution is to use fal's bigquery implementation here
            sleep(4)

            return AdapterResponse("OK", rows_affected=rows_affected)


def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    """Generic version of the read_df_from_relation."""

    if adapter.type() == "snowflake":
        import dbt.adapters.fal_experimental.support.snowflake as support_snowflake

        return support_snowflake.read_relation_as_df(adapter, relation)

    elif adapter.type() == "duckdb":
        import dbt.adapters.fal_experimental.support.duckdb as support_duckdb

        return support_duckdb.read_relation_as_df(adapter, relation)

    else:
        with new_connection(adapter, "fal:read_relation_as_df") as connection:
            alchemy_engine = _get_alchemy_engine(adapter, connection)
            return pd.read_sql_table(
                con=alchemy_engine,
                table_name=relation.identifier,
                schema=relation.schema,
            )


def prepare_for_adapter(adapter: BaseAdapter, function: Any) -> Any:
    """Prepare the given function to be used with string-like inputs
    (for relations) on the given adapter."""

    @functools.wraps(function)
    def wrapped(quoted_relation: str, *args, **kwargs) -> Any:
        relation = adapter.Relation.create(
            *quoted_relation.split("."), type=RelationType.Table
        )
        return function(adapter, relation, *args, **kwargs)

    return wrapped


def reconstruct_adapter(config: RuntimeConfig) -> BaseAdapter:
    from dbt.tracking import do_not_track

    # Prepare the DBT to not to track us.
    do_not_track()

    # Prepare the plugin loading system to handle the adapter.
    factory.load_plugin(config.credentials.type)
    factory.load_plugin(config.python_adapter_credentials.type)
    factory.register_adapter(config)

    # Initialize the adapter
    db_adapter = factory.get_adapter(config)
    reload_adapter_cache(db_adapter, config)

    return db_adapter


def reload_adapter_cache(adapter: BaseAdapter, config: RuntimeConfig) -> None:
    manifest = ManifestLoader.get_full_manifest(config)
    with new_connection(adapter, "fal:reload_adapter_cache"):
        adapter.set_relations_cache(manifest, True)


@contextmanager
def new_connection(adapter: BaseAdapter, connection_name: str) -> Connection:
    with adapter.connection_named(connection_name):
        yield adapter.connections.get_thread_connection()
