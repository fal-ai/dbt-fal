from __future__ import annotations

from typing import Any

import sqlalchemy
from dbt.adapters.base import BaseAdapter
from dbt.adapters.trino.connections import TrinoCredentials
from trino.sqlalchemy import URL


def create_engine(adapter: BaseAdapter) -> Any:
    creds = adapter.config.credentials._db_creds

    connect_args = _build_connect_args(creds)

    url = URL(host=creds.host, port=creds.port, catalog=creds.database, user=creds.user)
    return sqlalchemy.create_engine(url, connect_args=connect_args)


def _build_connect_args(credentials: TrinoCredentials) -> dict[str, Any]:
    # See:
    # https://github.com/starburstdata/dbt-trino/blob/master/dbt/adapters/trino/connections.py
    return {
        "auth": credentials.trino_auth(),
        "http_scheme": credentials.http_scheme.value,
    }
