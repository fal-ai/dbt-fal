from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.fal_experimental.adapter_support import new_connection
import pandas as pd
from dbt.adapters.sql import SQLAdapter
import duckdb


def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    db_path = adapter.config.credentials.path

    con = duckdb.connect(database=db_path)
    df = con.execute(f"SELECT * FROM {relation.schema}.{relation.identifier}").fetchdf()
    return df


def write_df_to_relation(
    adapter: SQLAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
) -> AdapterResponse:

    db_path = adapter.config.credentials.path    
    con = duckdb.connect(database=db_path)
    rows_affected = con.execute(
        f"CREATE OR REPLACE TABLE {relation.schema}.{relation.identifier} AS SELECT * FROM data;"
    ).fetchall()[0][0]
    return AdapterResponse("OK", rows_affected=rows_affected)
