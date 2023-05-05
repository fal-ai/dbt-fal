import functools
from typing import Any

import pandas as pd
import sqlalchemy
from contextlib import contextmanager
from dbt.adapters.base import BaseAdapter, BaseRelation, RelationType
from dbt.adapters.base.connections import AdapterResponse, Connection
from dbt.config import RuntimeConfig
from dbt.parser.manifest import MacroManifest, Manifest
from dbt.flags import Namespace

from dbt.adapters import factory

_SQLALCHEMY_DIALECTS = {
    "sqlserver": "mssql+pyodbc",
}


def _get_alchemy_engine(adapter: BaseAdapter, connection: Connection) -> Any:
    # The following code heavily depends on the implementation
    # details of the known adapters, hence it can't work for
    # arbitrary ones.
    adapter_type = adapter.type()

    sqlalchemy_kwargs = {}
    format_url = lambda url: url

    if adapter_type == "trino":
        import dbt.adapters.fal_experimental.support.trino as support_trino

        return support_trino.create_engine(adapter)

    elif adapter_type == "sqlserver":
        sqlalchemy_kwargs["creator"] = lambda *args, **kwargs: connection.handle
        url = _SQLALCHEMY_DIALECTS.get(adapter_type, adapter_type) + "://"
        url = format_url(url)
    else:
        message = (
            f"dbt-fal does not support {adapter_type} adapter. ",
            f"If you need {adapter_type} support, you can create an issue ",
            "in our GitHub repository: https://github.com/fal-ai/fal. ",
            "We will look into it ASAP.",
        )
        raise NotImplementedError(message)

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

    adapter_type = adapter.type()

    if adapter_type == "snowflake":
        import dbt.adapters.fal_experimental.support.snowflake as support_snowflake

        return support_snowflake.write_df_to_relation(adapter, dataframe, relation)

    elif adapter_type == "bigquery":
        import dbt.adapters.fal_experimental.support.bigquery as support_bq

        return support_bq.write_df_to_relation(adapter, dataframe, relation)

    elif adapter_type == "duckdb":
        import dbt.adapters.fal_experimental.support.duckdb as support_duckdb

        return support_duckdb.write_df_to_relation(adapter, dataframe, relation)

    elif adapter_type == "postgres":
        import dbt.adapters.fal_experimental.support.postgres as support_postgres

        return support_postgres.write_df_to_relation(adapter, dataframe, relation)
    elif adapter.type() == "athena":
        import dbt.adapters.fal_experimental.support.athena as support_athena

        return support_athena.write_df_to_relation(
            adapter, dataframe, relation, if_exists
        )
    elif adapter_type == "redshift":
        import dbt.adapters.fal_experimental.support.redshift as support_redshift

        return support_redshift.write_df_to_relation(adapter, dataframe, relation)

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

            return AdapterResponse("OK", rows_affected=rows_affected)


def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    """Generic version of the read_df_from_relation."""

    adapter_type = adapter.type()

    if adapter_type == "snowflake":
        import dbt.adapters.fal_experimental.support.snowflake as support_snowflake

        return support_snowflake.read_relation_as_df(adapter, relation)

    elif adapter_type == "bigquery":
        import dbt.adapters.fal_experimental.support.bigquery as support_bq

        return support_bq.read_relation_as_df(adapter, relation)

    elif adapter_type == "duckdb":
        import dbt.adapters.fal_experimental.support.duckdb as support_duckdb

        return support_duckdb.read_relation_as_df(adapter, relation)

    elif adapter_type == "postgres":
        import dbt.adapters.fal_experimental.support.postgres as support_postgres

        return support_postgres.read_relation_as_df(adapter, relation)

    elif adapter.type() == "athena":
        import dbt.adapters.fal_experimental.support.athena as support_athena

        return support_athena.read_relation_as_df(adapter, relation)

    elif adapter.type() == "redshift":
        import dbt.adapters.fal_experimental.support.redshift as support_redshift

        return support_redshift.read_relation_as_df(adapter, relation)

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
        # HACK: we need to drop the quotes from the relation parts
        # This was introduced in https://github.com/dbt-labs/dbt-core/pull/7115
        # and the recommended solution would be to create a macro `fal__resolve_model_name`
        # but it is not possible thanks a macro resolution error we get by returning the db_adapter type.
        # The overall solution could be to avoid creating a Relation and just passing the string as is to the read/write functions.
        parts = map(
            lambda part: part.strip(adapter.Relation.quote_character),
            [*quoted_relation.split(".")],
        )

        relation = adapter.Relation.create(*parts, type=RelationType.Table)
        return function(adapter, relation, *args, **kwargs)

    return wrapped


def reconstruct_adapter(
    flags: Namespace,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
) -> BaseAdapter:
    from dbt.flags import set_flags
    from dbt.tracking import do_not_track

    # Avoid dbt tracking
    do_not_track()

    # Flags need to be set before any plugin is loaded
    set_flags(flags)

    # Prepare the plugin loading system to handle the adapter
    factory.load_plugin(config.credentials.type)
    factory.load_plugin(config.python_adapter_credentials.type)
    factory.register_adapter(config)

    # Initialize the adapter
    db_adapter = factory.get_adapter(config)
    db_adapter._macro_manifest_lazy = macro_manifest
    reload_adapter_cache(db_adapter, manifest)

    return db_adapter


def reload_adapter_cache(adapter: BaseAdapter, manifest: Manifest) -> None:
    with new_connection(adapter, "fal:reload_adapter_cache"):
        adapter.set_relations_cache(manifest, True)


@contextmanager
def new_connection(adapter: BaseAdapter, connection_name: str) -> Connection:
    with adapter.connection_named(connection_name):
        yield adapter.connections.get_thread_connection()
