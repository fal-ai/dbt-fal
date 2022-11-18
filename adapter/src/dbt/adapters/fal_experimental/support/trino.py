from typing import Any, Dict
from dbt.adapters.base import BaseAdapter, Credentials
from trino.sqlalchemy import URL
from dbt.adapters.trino.connections import TrinoCredentials
import sqlalchemy

def create_engine(adapter: BaseAdapter) -> Any:
    creds = adapter.config.credentials._db_creds

    connect_args = _build_connect_args(creds)

    url = URL(
        host=creds.host,
        port=creds.port,
        catalog=creds.database,
        user=creds.user
    )
    return sqlalchemy.create_engine(url, connect_args=connect_args)

def _build_connect_args(credentials: TrinoCredentials) -> Dict[str, Any]:
    # See:
    # https://github.com/starburstdata/dbt-trino/blob/master/dbt/adapters/trino/connections.py
    return {
        "auth": credentials.trino_auth(),
        "http_scheme": credentials.http_scheme.value
    }
