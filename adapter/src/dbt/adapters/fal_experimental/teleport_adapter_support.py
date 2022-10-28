from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.fal_experimental.connections import TeleportCredentials, TeleportTypeEnum

from dbt.fal.adapters.teleport.impl import TeleportAdapter

def wrap_db_adapter(db_adapter: BaseAdapter, teleport_credentials: TeleportCredentials) -> TeleportAdapter:

    if TeleportAdapter.is_teleport_adapter(db_adapter):
        return db_adapter

    # Wrap the adapter with a custom implementation
    if db_adapter.type() == 'duckdb':
        import dbt.adapters.fal_experimental.teleport_support.duckdb as support_duckdb
        return support_duckdb.DuckDBAdapterTeleport(db_adapter, teleport_credentials)

    if db_adapter.type() == 'snowflake':
        import dbt.adapters.fal_experimental.teleport_support.snowflake as support_snowflake
        if teleport_credentials.type == TeleportTypeEnum.REMOTE_S3:
            return support_snowflake.SnowflakeAdapterTeleport(db_adapter, teleport_credentials)
        else:
            raise RuntimeError("Snowflake teleporting works only with S3.")

    raise NotImplementedError(f"Teleport support has not been implemented for adapter {db_adapter.type()}")
