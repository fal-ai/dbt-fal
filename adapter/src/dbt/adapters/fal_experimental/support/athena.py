from typing import Any
import sqlalchemy
from dbt.adapters.base import BaseAdapter
from urllib.parse import quote_plus


def create_engine(adapter: BaseAdapter) -> Any:
    creds = adapter.config.credentials._db_creds
    conn_str = ("awsathena+rest://:@athena.{region_name}.amazonaws.com:443/"
                "{schema_name}?s3_staging_dir={s3_staging_dir}"
                "&location={location}&compression=snappy")
    return sqlalchemy.create_engine(conn_str.format(
        region_name=creds.region_name,
        schema_name=creds.schema,
        s3_staging_dir=quote_plus(creds.s3_staging_dir),
        location=quote_plus(creds.s3_staging_dir)))
