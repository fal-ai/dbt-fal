from dbt.adapters.base import BaseAdapter, BaseRelation
from dbt.adapters.base.connections import AdapterResponse
from dbt.adapters.fal_experimental.adapter_support import new_connection
import pandas as pd
from dbt.adapters.sql import SQLAdapter


def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    sql = f"SELECT * FROM {relation}"

    assert adapter.type() == "snowflake"

    with new_connection(adapter, "fal-snowflake:read_relation_as_df") as conn:
        cur = conn.handle.cursor()
        cur.execute(sql)
        df: pd.DataFrame = cur.fetch_pandas_all()

        # HACK: manually parse ARRAY and VARIANT since they are returned as strings right now
        # Related issue: https://github.com/snowflakedb/snowflake-connector-python/issues/544
        for desc in cur.description:
            # 5=VARIANT, 10=ARRAY -- https://docs.snowflake.com/en/user-guide/python-connector-api.html#type-codes
            if desc.type_code in [5, 10]:
                import json

                df[desc.name] = df[desc.name].map(lambda v: json.loads(v))

        return df


def write_df_to_relation(
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

    with new_connection(adapter, "fal-snowflake:write_df_to_relation") as conn:
        connection_manager: SnowflakeConnectionManager = _adapter.connections  # type: ignore

        with connection_manager.exception_handler("LOAD TABLE"):
            success, _, num_rows, output = snowflake_pandas.write_pandas(
                conn.handle,
                data,
                table_name=table,
                database=database,
                schema=schema,
                overwrite=True,  # TODO: This helps when table schema changes, but it is not atomic
                auto_create_table=True,
                quote_identifiers=False,
            )
            if not success:
                # In case the failure does not raise by itself
                # I have not been able to reproduce such a case
                from dbt.exceptions import DatabaseException

                raise DatabaseException(output)

            # TODO: better AdapterResponse
            return AdapterResponse(str(output[0][1]), rows_affected=num_rows)
