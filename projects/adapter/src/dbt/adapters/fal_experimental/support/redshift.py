import csv
from io import StringIO

import pandas as pd
import sqlalchemy
import awswrangler as wr

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.connections import AdapterResponse

from dbt.adapters.fal_experimental.adapter_support import new_connection, drop_relation_if_it_exists

from dbt.adapters.redshift import RedshiftAdapter


def read_relation_as_df(
    adapter: RedshiftAdapter, relation: BaseRelation
) -> pd.DataFrame:
    sql = f"SELECT * FROM {relation}"

    assert adapter.type() == "redshift"

    with new_connection(adapter, "fal-redshift:read_relation_as_df") as conn:
        df = wr.redshift.read_sql_query(sql, con=conn.handle)
        return df


def write_df_to_relation(
    adapter: RedshiftAdapter,
    data: pd.DataFrame,
    relation: BaseRelation,
) -> AdapterResponse:

    assert adapter.type() == "redshift"

    with new_connection(adapter, "fal-redshift:write_df_to_relation") as connection:
        # TODO: this should probably live in the materialization macro.
        temp_relation = relation.replace_path(
            identifier=f"__dbt_fal_temp_{relation.identifier}"
        )
        drop_relation_if_it_exists(adapter, temp_relation)

        wr.redshift.to_sql(
            data,
            connection.handle,
            table=temp_relation.identifier,
            schema=temp_relation.schema,
            index=False,
            varchar_lengths_default=65535
        )

        adapter.cache.add(temp_relation)
        drop_relation_if_it_exists(adapter, relation)
        adapter.rename_relation(temp_relation, relation)
        adapter.commit_if_has_connection()

        return AdapterResponse("OK")
