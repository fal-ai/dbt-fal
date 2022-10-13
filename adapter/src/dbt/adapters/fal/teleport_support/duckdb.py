from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.impl import BaseAdapter

from dbt.fal.adapters.teleport.impl import TeleportAdapter
from dbt.fal.adapters.teleport.info import TeleportInfo


class DuckDBAdapterTeleport(TeleportAdapter):

    def __init__(self, db_adapter: BaseAdapter):
        self._db_adapter = db_adapter
        with self._db_adapter.connection_named('teleport:init'):
            self._db_adapter.execute("INSTALL 'parquet'")
            self._db_adapter.execute("LOAD 'parquet'")

    @classmethod
    def storage_formats(cls):
        return ['parquet']

    def teleport_from_external_storage(self, relation: BaseRelation, relation_path: str, teleport_info: TeleportInfo):
        assert teleport_info.format == 'parquet', "duckdb only supports parquet format for Teleport"

        url = teleport_info.build_url(relation_path)

        with self._db_adapter.connection_named('teleport:copy_from'):
            rendered_macro = self._db_adapter.execute_macro('duckdb__copy_from', kwargs={'relation': relation, 'url': url})
            self._db_adapter.execute(rendered_macro)

    def teleport_to_external_storage(self, relation: BaseRelation, teleport_info: TeleportInfo):
        assert teleport_info.format == 'parquet', "duckdb only supports parquet format for Teleport"

        rel_path = teleport_info.build_relation_path(relation)
        url = teleport_info.build_url(rel_path)

        with self._db_adapter.connection_named('teleport:copy_to'):
            rendered_macro = self._db_adapter.execute_macro('duckdb__copy_to', kwargs={'relation': relation, 'url': url})
            self._db_adapter.execute(rendered_macro)

        return rel_path
