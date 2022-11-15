from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.fal_experimental.adapter_support import new_connection
import pandas as pd
from dbt.adapters.sql import SQLAdapter
from trino.dbapi import connect


def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    creds = adapter.config.credentials._db_creds
    with connect(
        host=creds.host,
        port=creds.port,
        user=creds.user,
        catalog=creds.database,
        schema=creds.schema,
    ) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {relation.schema}.{relation.identifier}")
        return pd.DataFrame(cur.fetchall(), columns=[i[0] for i in cur.description])


def write_df_to_relation(
    adapter: SQLAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
) -> AdapterResponse:
    import sqlalchemy
    creds = adapter.config.credentials._db_creds
    url = f'trino://{creds.user}@{creds.host}:{creds.port}/{creds.database}'
    engine = sqlalchemy.create_engine(url)
    with engine.connect() as conn:
        rows_affected = data.to_sql(relation.identifier, conn, schema=relation.schema, if_exists='replace', index=False)
        return AdapterResponse("OK", rows_affected=rows_affected)
