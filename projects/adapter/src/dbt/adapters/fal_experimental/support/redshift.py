from typing import Any
from dbt.adapters.base import BaseAdapter

from dbt.adapters.redshift.connections import RedshiftConnectMethodFactory


def create_engine(adapter: BaseAdapter) -> Any:
    creds = adapter.config.credentials

    connect_factory = RedshiftConnectMethodFactory(creds)
    connect = connect_factory.get_connect_method()

    return connect()
