from typing import Any
import six
from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.connection import AdapterResponse
from dbt.adapters.fal.wrappers import FalCredentialsWrapper
import sqlalchemy
import pandas as pd
from dbt.adapters.base import BaseAdapter
from urllib.parse import quote_plus


def create_engine(adapter: BaseAdapter) -> Any:
    if isinstance(adapter.config.credentials, FalCredentialsWrapper):
        creds = adapter.config.credentials._db_creds
    else:
        # In isolated environment, credentials are AthenaCredentials
        creds = adapter.config.credentials
    conn_str = ("awsathena+rest://:@athena.{region_name}.amazonaws.com:443/"
                "{schema_name}?s3_staging_dir={s3_staging_dir}"
                "&location={location}&compression=snappy")
    return sqlalchemy.create_engine(conn_str.format(
        region_name=creds.region_name,
        schema_name=creds.schema,
        s3_staging_dir=quote_plus(creds.s3_staging_dir),
        location=quote_plus(creds.s3_staging_dir)))


def drop_relation_if_it_exists(adapter: BaseAdapter, relation: BaseRelation) -> None:
    if adapter.get_relation(
        database=relation.database,
        schema=relation.schema,
        identifier=relation.identifier,
    ):
        adapter.drop_relation(relation)


def write_df_to_relation(adapter, dataframe, relation, if_exists) -> AdapterResponse:

    assert adapter.type() == "athena"
    if isinstance(adapter.config.credentials, FalCredentialsWrapper):
        creds = adapter.config.credentials._db_creds
    else:
        # In isolated environment, credentials are AthenaCredentials
        creds = adapter.config.credentials

    # This is a quirk of dbt-athena-community, where they set
    # relation.schema = relation.identifier
    temp_relation = relation.replace_path(
        schema=relation.database,
        database=creds.database,
        # athena complanes when table location has x.__y
        identifier=f"dbt_fal_temp_{relation.schema}"
    )

    relation = temp_relation.replace_path(identifier=relation.schema)

    drop_relation_if_it_exists(adapter, temp_relation)

    alchemy_engine = create_engine(adapter)

    rows_affected = dataframe.to_sql(
        con=alchemy_engine,
        name=temp_relation.identifier,
        schema=temp_relation.schema,
        if_exists=if_exists,
        index=False,
    )

    adapter.cache.add(temp_relation)

    drop_relation_if_it_exists(adapter, relation)


    # athena doesn't let us rename relations, so we do it by hand
    stmt = f"create table {relation} as select * from {temp_relation} with data"
    adapter.execute(six.text_type(stmt).strip())
    adapter.cache.add(relation)
    adapter.drop_relation(temp_relation)

    adapter.commit_if_has_connection()
    return AdapterResponse("OK", rows_affected=rows_affected)

def read_relation_as_df(adapter: BaseAdapter, relation: BaseRelation) -> pd.DataFrame:
    alchemy_engine = create_engine(adapter)

    # This is dbt-athena-community quirk, table_name=relation.schema

    return pd.read_sql_table(
        con=alchemy_engine,
        table_name=relation.schema,
        schema=relation.database,
    )
