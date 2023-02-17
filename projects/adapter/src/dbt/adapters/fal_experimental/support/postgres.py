from io import StringIO

import pandas as pd

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.postgres import PostgresAdapter


def read_relation_as_df(
    adapter: PostgresAdapter, relation: BaseRelation
) -> pd.DataFrame:
    assert adapter.type() == "postgres"

    with new_connection(adapter, "fal-postgres:read_relation_as_df") as connection:
        alchemy_engine = _get_alchemy_engine(adapter, connection)
        return pd.read_sql_table(
            con=alchemy_engine,
            table_name=relation.identifier,
            schema=relation.schema,
        )


def write_df_to_relation(
    adapter: PostgresAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
) -> AdapterResponse:
    assert adapter.type() == "postgres"

    with new_connection(adapter, "fal-postgres:write_df_to_relation") as connection:
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
            method=_psql_insert_copy,
        )
        adapter.cache.add(temp_relation)
        drop_relation_if_it_exists(adapter, relation)
        adapter.rename_relation(temp_relation, relation)
        adapter.commit_if_has_connection()

        return AdapterResponse("OK", rows_affected=rows_affected)


def _psql_insert_copy(table, conn, keys, data_iter):
    """Alternative to_sql method for PostgreSQL.

    Adapted from https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join((f'"{k}"' for k in keys))
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)
