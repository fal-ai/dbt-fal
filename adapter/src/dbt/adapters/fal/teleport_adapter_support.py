from dbt.adapters.base.impl import BaseAdapter

from dbt.fal.adapters.teleport.impl import TeleportAdapter

def wrap_db_adapter(db_adapter: BaseAdapter) -> TeleportAdapter:

    if TeleportAdapter.is_teleport_adapter(db_adapter):
        return db_adapter

    # Wrap the adapter with a custom implementation
    if db_adapter.type() == 'duckdb':
        import dbt.adapters.fal.teleport_support.duckdb as support_duckdb
        return support_duckdb.DuckDBAdapterTeleport(db_adapter)

    raise NotImplementedError(f"Teleport support has not been implemented for adapter {db_adapter.type()}")

